package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.event.LoggingReceive
import scala.language.postfixOps
import scala.concurrent.{ Await, Future, TimeoutException }

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the persistence object
  var persistence: ActorRef = context.actorOf(persistenceProps)
  
  var leaderAcks = Map.empty[Long, (ActorRef, OperationAck)]
  var replicaAcks = Map.empty[Long, (ActorRef, SnapshotAck)]
  var leaderAcked = Seq[Long]()
  var replicaAcked = Seq[Long]()
  
  var repAcks = Map.empty[Long, ActorRef]
  var repAcked = Map.empty[Long, Long]
  
  var opLog = Seq[Operation]()

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {
    case msg @ Insert(key: String, value: String, id: Long) => {
      kv = kv + ((key, value))
      opLog ++= Seq(msg)
      replicate(sender, key, Some(value), id)
      persistLeader(sender, key, Some(value), id)
    }
    case msg @ Remove(key: String, id: Long) => {
      kv = kv - key
      opLog ++= Seq(msg)
      replicate(sender, key, None, id)
      persistLeader(sender, key, None, id)
    }
    case Get(key: String, id: Long) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Replicas(replicas: Set[ActorRef]) => {
      secondaries.filter { secondary => !replicas.contains(secondary._1) }
      .foreach { secondary =>
        secondaries -= secondary._1
        replicators -= secondary._2
        context.stop(secondary._2)
      }
      
      replicas.filter { replica => replica != self }
      .foreach { replica =>
        val replicator = context.actorOf(Replicator.props(replica))
        secondaries += (replica -> replicator)
        replicators += replicator
        opLog.foreach { 
          case Insert(key: String, value: String, id: Long) => replicator ! replicate(self, key, Some(value), id) 
          case Remove(key: String, id: Long) => replicator ! replicate(self, key, None, id) 
          case _ => 
        }
      }
    }
    case Replicated(key: String, id: Long) => {
      if (repAcked.contains(id)) {
        repAcked = repAcked.updated(id, repAcked(id) + 1)
      }
      else {
        repAcked += (id -> 1)
      }
      if (isGloballyAcknowledged(id) && isPersisted(id)) {
        repAcks(id) match {
          case (originalSender: ActorRef) => 
            originalSender ! OperationAck(id)
        }
      }
    }
    case Persisted(key: String, id: Long) => { leaderAcks(id) match {
      case (originalSender: ActorRef, OperationAck(id: Long)) => 
        leaderAcked ++= Seq(id) 
        if (isGloballyAcknowledged(id)) {
          originalSender ! OperationAck(id)
        }
      }
    }
  }

  /* TODO Behavior for the replica role. */
  var nextSeq: Long = 0
  val replica: Receive = LoggingReceive {
    case Get(key: String, id: Long) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Snapshot(key: String, valueOption: Option[String], seq: Long) => {
      if (seq == nextSeq) {
        valueOption match {
          case Some(value) => {
            kv = kv + (key -> value)
            persistReplica(sender, key, Some(value), seq)
          }
          case None => {
            kv = kv - key
            persistReplica(sender, key, None, seq)
          }
        }
        nextSeq += 1
      }
      else if (seq < nextSeq) {
        sender ! SnapshotAck(key, seq)
      }
      else { 
      }
    }
    case Persisted(key: String, id: Long) => { replicaAcks(id) match {
      case (originalSender: ActorRef, SnapshotAck(key: String, id: Long)) => 
        replicaAcked ++= Seq(id) 
        originalSender ! SnapshotAck(key, id)
      }
    }
  }
  
  def replicate(sender: ActorRef, key: String, valueOption: Option[String], id: Long) = {
    repAcks += (id -> sender)
    if (replicators.size > 0) {
      replicators.foreach{ replicator => 
        replicator ! Replicate(key, valueOption, id)
      }
      context.system.scheduler.scheduleOnce(1 second) {
        if (!isGloballyAcknowledged(id)) {
          repAcks(id) match {
            case (originalSender: ActorRef) => 
              originalSender ! OperationFailed(id)
          }
        }
      }
    }
    else {
      self ! Replicated(key, id)
    }
  }
  
  def persistLeader(sender: ActorRef, key: String, valueOption: Option[String], id: Long) = {
    leaderAcks += (id -> (sender, OperationAck(id)))
    val cancellable = context.system.scheduler.schedule(0 millis, 100 millis) {
      if (!leaderAcked.contains(id)) {
        persistence ! Persist(key, valueOption, id)
      }
    }
    context.system.scheduler.scheduleOnce(1 second) {
      cancellable.cancel()
      if (!leaderAcked.contains(id)) {
        leaderAcks(id) match {
          case (originalSender: ActorRef, OperationAck(id: Long)) => 
            leaderAcked ++= Seq(id) 
            originalSender ! OperationFailed(id)
        }
      }
    }
  }
  
  def persistReplica(sender: ActorRef, key: String, valueOption: Option[String], id: Long) = {
    replicaAcks += (id -> (sender, SnapshotAck(key, id)))
    val cancellable = context.system.scheduler.schedule(0 millis, 100 millis) {
      if (!replicaAcked.contains(id)) {
        persistence ! Persist(key, valueOption, id)
      }
    }
  }
  
  def isGloballyAcknowledged(id: Long) = {
    repAcked.contains(id) && repAcked(id) >= replicators.size
  }
  
  def isPersisted(id: Long) = {
    leaderAcked.contains(id)
  }

}


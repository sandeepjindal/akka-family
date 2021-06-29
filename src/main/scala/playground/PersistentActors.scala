package playground

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import java.util.Date

object PersistentActors extends App{

  /**
    * Scenario: We have a business and an accountant which keep track
    * of invoices
    */
  //COMMANDS
  case class Invoice(recipient: String, date: Date, amount: Int)

  case class InvoiceBulk(invoices:List[Invoice])

  //EVENTS
  case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant extends PersistentActor with ActorLogging {

    var latestInvoiceId = 0
    var totalAmount =0

    override def persistenceId: String = "simple-accountant"

    /**
      * Normal recieve method
      * @return
      */
    override def receiveCommand: Receive = {
      case Invoice(recipient,date,amount) =>

      /**
        * when we recieve a command
        * 1. create a event to persist into the store
        * 2. persist the event and pass in a callback that will get triggered once the event is written
        * 3. We update the actor state once event is persisted
        */
      log.info(s"Recieved invoide with amount ${amount}")
      val event = InvoiceRecorded(latestInvoiceId , recipient, date, amount)
      persist(event)
      /* time gap: all the other messages sent to this actor are STASHED */ { e =>
      // update state
        latestInvoiceId +=1
        totalAmount +=amount

        // correctly identify the sender of the COMMAND
        sender() ! "persistanceAck"
        log.info(s"Persisted $e as invoice # ${e.id}, for total amount ${totalAmount}")
      }
      // act like a normal actor
      case InvoiceBulk(invoices) =>
        val invoicesIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoicesIds).map{ pair => {
          val id = pair._2
          val invoice = pair._1
          InvoiceRecorded(id,invoice.recipient,invoice.date,invoice.amount)
        }}
        persistAll(events) { e =>
          latestInvoiceId +=1
          totalAmount += e.amount
          log.info(s"Persisted SINGLE $e as invoice # ${e.id}, for total amount ${totalAmount}")

        }
      case "print" =>
        log.info(s"Latest invoice id $latestInvoiceId, total Amount: $totalAmount")
    }

    /**
      * handler which will be called on recovery
      * @return
      */
    override def receiveRecover: Receive = {
      /**
        * follow the logic of persist steps if reciecve command
        */
      case InvoiceRecorded(id,_,_,amount) => {
        log.info(s"recovered invoice # ${id} for amount $totalAmount")
        latestInvoiceId = id
        totalAmount +=amount
      }
    }

    /**
      * This method is called if persisting failed.
      * The actor will be STOPPED.
      *
      * Best practice: start the actor again after a while use backoff supervisor
      * @param cause
      * @param event
      * @param seqNr
      */
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit ={
      log.error(s"failed to persist the event $event because of $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    /**
      * If journal fails to persist the event
      * The actor is resumed.
      * @param cause
      * @param event
      * @param seqNr
      */
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist rejected for event $event because of $cause")
      super.onPersistRejected(cause, event, seqNr)
    }

  }

  val system = ActorSystem("PersistantActors")
  val accountant = system.actorOf(Props[Accountant], "simpleAccountant")

//  for(i <- 1 to 10){
//    accountant ! Invoice("the food company", new Date, i * 1000)
//  }
  val newInvoices = for(i <- 1 to 5 ) yield Invoice("The medicines", new Date, i * 2000)

  accountant ! InvoiceBulk(newInvoices.toList)
  /**
    * persistance failures
    *
    * persistance of multiple events
    *
    * NEVER EVER CALL PERSIST OR PERSIST FROM FUTURES.
    *
    * shutdown of persistent actors
    *
    */

}

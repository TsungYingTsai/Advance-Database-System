import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;

    public LockManager() {
        this.resourceToLock = new HashMap<Resource, ResourceLock>();

    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    //TODO: 4/21
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {

        //If a blocked transaction calls acquire
        if (transaction.getStatus().equals(Transaction.Status.Waiting)) {
            throw new IllegalArgumentException();
        }

        //If a transaction requests a lock that it already holds
        if (holds(transaction,resource,lockType)) {
            throw new IllegalArgumentException();
        }

        //deal with adding Page
        if (resource.getResourceType().equals(Resource.ResourceType.PAGE)){
            if (lockType.equals(LockType.IS) || lockType.equals(LockType.IX)) {throw new IllegalArgumentException();}

            else if (lockType.equals(LockType.S)) {
                Page thePage = (Page) resource;
                Table checkTable = thePage.getTable();
                ResourceLock checkRes = this.resourceToLock.get(checkTable);
                if (checkRes == null) {throw new IllegalArgumentException();}
                int i = 0;
                //System.out.println(checkRes.lockOwners);
                if (checkRes.lockOwners.isEmpty() == false) {
                    for (Request rr : checkRes.lockOwners) {
                        if (rr.lockType.equals(LockType.IS) || rr.lockType.equals(LockType.IX)) {
                            i = 1;
                        }
                    }
                }
                if (i == 0) {throw new IllegalArgumentException();}
            }
            else if (lockType.equals(LockType.X)) {
                Page thePage = (Page) resource;
                Table checkTable = thePage.getTable();
                ResourceLock checkRes = this.resourceToLock.get(checkTable);
                if (checkRes == null) {throw new IllegalArgumentException();}
                int i = 0;
                if (checkRes.lockOwners.isEmpty() == false) {
                    for (Request rr : checkRes.lockOwners) {
                        if (rr.lockType.equals(LockType.IX)) {
                            i = 1;
                        }
                    }
                }
                if (i == 0) {throw new IllegalArgumentException();}
            }

        }

        // instantly upgrade for (s --> x) woth same table
        if (lockType.equals(LockType.X)) {
            if (resourceToLock.containsKey(resource) == true) {
                if (resourceToLock.get(resource).lockOwners.size() > 0) {
                    ArrayList<Request> lockArray = resourceToLock.get(resource).lockOwners;
                    if (lockArray.size() == 1) {
                        Request one = lockArray.get(0);
                        if (one.lockType.equals(LockType.S)) {
                            if (one.transaction.equals(transaction)) {
                                resourceToLock.get(resource).lockOwners.clear();
                                resourceToLock.get(resource).lockOwners.add(new Request(transaction, lockType));
                            }
                        }

                    }
                }
            }
        }

        // if want to downgrade (x -> s) --> Illegal
        if (lockType.equals(LockType.S)) {
            if (resourceToLock.containsKey(resource) == true) {
                for (Request rr : resourceToLock.get(resource).lockOwners) {
                    if (rr.equals(new Request(transaction, LockType.X))) {
                        throw new IllegalArgumentException();
                    }
                }
            }
        }



        if (compatible(resource,transaction,lockType)) {
            if (resourceToLock.containsKey(resource) == false) {
                Request Req = new Request(transaction,lockType);
                ResourceLock RLock = new ResourceLock();
                RLock.lockOwners.add(Req);
                this.resourceToLock.put(resource, RLock);
            }
            else {
                ResourceLock exitedRLock = resourceToLock.get(resource);
                exitedRLock.lockOwners.add(new Request(transaction,lockType));
            }
        }
        else {
            transaction.sleep();
            if (resourceToLock.containsKey(resource) == false) {
                assert (1 == 2);
                Request Req = new Request(transaction,lockType);
                ResourceLock RLock = new ResourceLock();
                RLock.requestersQueue.add(Req);
                this.resourceToLock.put(resource, RLock);
            }
            else {
                ResourceLock exitedRLock = resourceToLock.get(resource);
                exitedRLock.requestersQueue.add(new Request(transaction,lockType));
            }

            //Request Req = new Request(transaction,lockType);
            //ResourceLock RLock = new ResourceLock();
            //RLock.requestersQueue.add(Req);
        }
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    //TODO: 4/21
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {
        // must compatible if nothing in exited ResourceToLock
        if (resourceToLock.containsKey(resource) == false) {return true;}

        // deal wth all X
        //if (lockType.equals(LockType.X)) {return false;}

        // deal with others
        return matrixCom(resourceToLock.get(resource).lockOwners, transaction, lockType);

    }

    private boolean matrixCom(ArrayList<Request> ListofExistedReq, Transaction transaction, LockType ReqlockType) {
        for (Request exitedRequest : ListofExistedReq) {
            if (ReqlockType.equals(LockType.X)) {return false;}
            if (exitedRequest.lockType.equals(LockType.X)) {return false;}
            if (ReqlockType.equals(LockType.S) && exitedRequest.lockType.equals(LockType.IX)) {return false;}
            if (ReqlockType.equals(LockType.IX) && exitedRequest.lockType.equals(LockType.S)) {return false;}
            //TODO: check
            // If a transaction that currently holds an X lock on a resource requests an S lock on the same resource (downgrade)
            if (ReqlockType.equals(LockType.S) && exitedRequest.lockType.equals(LockType.X)) {throw new IllegalArgumentException();}
            //TODO: check
            // If a transaction that currently holds an IX lock on a table requests an IS lock the same table (downgrade)
            if (ReqlockType.equals(LockType.IS) && exitedRequest.lockType.equals(LockType.IX)) {
                if (exitedRequest.transaction.equals(transaction)) {
                    throw new IllegalArgumentException();
                }
            }
        }
        return true;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{

        if (resourceToLock.get(resource) == null || resourceToLock.get(resource).lockOwners.size() == 0) {throw new IllegalArgumentException();}

        //If a blocked transaction calls release
        if (transaction.getStatus().equals(Transaction.Status.Waiting)) {
            throw new IllegalArgumentException();
        }

        //If the transaction doesn't hold any of the four possible lock types on this resource
        if ((holds(transaction,resource,LockType.S) || holds(transaction,resource,LockType.X)
                || holds(transaction,resource,LockType.IX) || holds(transaction,resource,LockType.IS)) == false) {throw new IllegalArgumentException();}

        //If a transaction is trying to release a table level lock without having released all the locks for the pages of that table first
        System.out.println(resourceToLock.keySet());
        for (Resource resour: resourceToLock.keySet()) {
            if (resour.getResourceType().equals(Resource.ResourceType.PAGE)) {
                System.out.println(transaction.getName());
                System.out.println(resourceToLock.get(resour));
                for (Request oo: resourceToLock.get(resour).lockOwners) {
                    if (transaction.equals(oo.transaction)) {
                        throw new IllegalArgumentException();
                    }
                }
            }
        }



        if (resourceToLock.containsKey(resource)) {
            //Request Req = new Request(transaction,lockType);
            ResourceLock rdToReleaseLock = resourceToLock.get(resource);
            //rdToReleaseLock.lockOwners.clear();
            //System.out.println(rdToReleaseLock.requestersQueue);

            // check if existed the same transaction in the resource. If yes, delete it
            for (Request rdtoRelRequest : rdToReleaseLock.lockOwners) {
                if (transaction.equals(rdtoRelRequest.transaction)) {
                    Request beingRM = rdtoRelRequest;
                    rdToReleaseLock.lockOwners.remove(rdtoRelRequest);
                    /*
                    if (rdToReleaseLock.lockOwners == null && rdToReleaseLock.requestersQueue != null) {
                        Request QueueFirst = rdToReleaseLock.requestersQueue.getFirst();

                    }
                    */
                    break;
                }
            }


            // deal with s replace by X if the same transaction and X
            if (rdToReleaseLock.requestersQueue.isEmpty() == false) {
                if (rdToReleaseLock.lockOwners.size() == 0) {
                    while (rdToReleaseLock.requestersQueue.isEmpty() == false) {
                        Request firstQue = rdToReleaseLock.requestersQueue.removeFirst();
                        //System.out.println(rdToReleaseLock.requestersQueue);
                        firstQue.transaction.wake();
                        acquire(firstQue.transaction, resource, firstQue.lockType);
                        if (firstQue.lockType.equals(LockType.X)) {
                            break;
                        }
                    }
                }
                else if (rdToReleaseLock.lockOwners.size() == 1) {
                    //System.out.println("INN");
                    //while (rdToReleaseLock.requestersQueue.isEmpty() == false) {
                    if (rdToReleaseLock.lockOwners.get(0).lockType.equals(LockType.S)){
                        Request thefirstQue = rdToReleaseLock.requestersQueue.getFirst();
                        System.out.println(rdToReleaseLock.lockOwners);
                        System.out.println(rdToReleaseLock.requestersQueue);
                        if (rdToReleaseLock.lockOwners.get(0).transaction.equals(thefirstQue.transaction) && thefirstQue.lockType.equals(LockType.X)) {
                            rdToReleaseLock.lockOwners.clear();

                            Request addfirstQue = rdToReleaseLock.requestersQueue.removeFirst();
                            //System.out.println(rdToReleaseLock.requestersQueue);
                            addfirstQue.transaction.wake();
                            acquire(addfirstQue.transaction, resource, addfirstQue.lockType);
                            //if (addfirstQue.lockType.equals(LockType.X)) {
                            //  break;
                        }
                    }
                    //}
                }
            }
            //System.out.println(rdToReleaseLock.requestersQueue);
        }
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         // HW5: To do
         return;
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    //TODO: 4/22
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        if (resourceToLock.containsKey(resource) == false) {return false;}
        else {
            Request checkedRq = new Request(transaction,lockType);
            ResourceLock existedRLock = resourceToLock.get(resource);
            for (Request existedRq: existedRLock.lockOwners) {

                assert (existedRq.getClass().equals(checkedRq.getClass()));

                if (existedRq.equals(checkedRq)) {return true;}
            }
            /*
            for (Request existedRq: existedRLock.requestersQueue) {

                assert (existedRq.getClass().equals(checkedRq.getClass()));

                if (existedRq.equals(checkedRq)) {return true;}
            }
            */

        }
        return false;
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}

from networkModel import Cluster, Message, MessageModel, Job
from math import log

def timeMessages(messages, data=32):
    times = []
    bandwidths = []
    for message in messages:
       messageTime = data/float(message.bandwidth)
       times.append(messageTime)
       bandwidths.append(message.bandwidth)

    return max(times)

def run2IslandMultiPairSim(verbose=False):
    print "\n ------------- Two island simultaneous pairs ---------- "
    cluster = Cluster(numIslands=2, nodesPerIsland=32)
    print cluster

    messageModel = MessageModel()

    print "\nPassing messages between pairs of nodes in groups of 2-64 simultaneously sending pairs. Pairs are located on separate islands."
    print "Number of pairs, Bandwidth (GB/s)"

    # iterate over separate communications involving increasing numbers of simultaneous messages
    for nr in range(2,64,2):
        job = Job(numRanks=nr)
        # split ranks in two onto two islands, but pack within an island
        job.allocateSplitIslands(cluster)
        if verbose:
            print job

        # calculate bandwidths per message when all ranks are paired off with a rank
        # nranks/2 away
        messageModel.setPairedMessages(job)
        messageModel.setBandwidths(cluster)
        if verbose:
            print messageModel

        # calculate total bandwidth 
        time = timeMessages(messageModel.messages)
        print str(nr) + ", " + str(time)

def runQuestDistributedSim(numIslands=3, numRanks=64, verbose=False, allocation="random"): 
    print "\n ------------- Many island QuEST sim ---------- "
    cluster = Cluster(numIslands=numIslands, nodesPerIsland=32)
    print cluster

    job = Job(numRanks=numRanks)
    if allocation == "random":
    	job.allocateRandom(cluster)
    elif allocation == "packed":
        job.allocatePacked(cluster)
    else:
        print "ERROR: Allocation type not recognised"
        return
    print job
    
    messageModel = MessageModel()

    print "\nRotating distributed qubits using", numRanks, "ranks across", numIslands, "islands."
    print "Distributed rotation qubit, Bandwidth (GB/s)"

    # iterate over distributed qubit number, where the first distributed qubit is called qubit 0
    print "qubit, time(s), bandwidth(GB/s)"
    for distributedQubit in range(int(log(numRanks, 2))):
        # calculate bandwidth per message when ranks are paired off between nodes
        # in the Quest pattern for distributed qubits
        messageModel.setPairedMessagesByDistance(job, pow(2, distributedQubit))
        messageModel.setBandwidths(cluster)

        if (verbose):
            print messageModel

        # calculate total bandwidth
        time = timeMessages(messageModel.messages)
        print str(distributedQubit) + ", " + str(time)


def main():
    #run2IslandMultiPairSim()

    #runQuestDistributedSim(numIslands=3, numRanks=32)
    
    runQuestDistributedSim(numIslands=2, numRanks=64, allocation="packed")

    
    


if __name__ == "__main__":
    main()




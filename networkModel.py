import numpy as np
from math import ceil, floor
from random import shuffle

class Cluster:
    """
    Immutable object containing properties of the cluster:
        numIslands
        nodesPerIsland
        islandBandwidth
        singleNodeBandwidth
    """
    def __init__(self, numIslands=2, nodesPerIsland=8, islandBandwidth=7, singleNodeBandwidth=4):
        self.numIslands = numIslands
        self.nodesPerIsland = nodesPerIsland
        self.islandBandwidth = islandBandwidth
        self.singleNodeBandwidth = singleNodeBandwidth

        self.numNodes = self.numIslands*self.nodesPerIsland

    def __str__(self):
	cString = "Cluster: " + str(self.numIslands) + " islands of " + str(self.nodesPerIsland) + " nodes.\n" \
		+ "Max bandwidth between nodes is " + str(self.singleNodeBandwidth) + "GB/s. Max bandwidth between islands is " \
		+ str(self.islandBandwidth) + "GB/s."
	return cString

""" 
Object holding properties of a single, unidirectional message passed from rank1 to rank2
    rank1: sender id
    rank2: receiver id
    host1: sender host id
    host2: receiver host id
    bandwidth: bandwidth at which the message will be sent
"""
class Message: 
    def __init__(self, rank1, rank2, host1, host2):
        self.rank1 = rank1
        self.rank2 = rank2
        self.host1 = host1
        self.host2 = host2
        self.bandwidth = 0

    def setBandwidth(self, bandwidth):
        self.bandwidth = bandwidth

    def setIslands(self, island1, island2):
        self.island1 = island1
        self.island2 = island2

    def __str__(self):
        return "ranks " + str(self.rank1) + " and " +\
            str(self.rank2) + " at " + str(self.bandwidth) + "GB/s"

"""
Object containing a collection of messages, represented by Message objects. 
Includes functions to set the bandwidth of those messages given the assumption that all
messages will be sent simultaneously
"""
class MessageModel:
    def __init__(self):
        self.messages = []

    def setPairedMessages(self, job):
        """ 
        Create a group of messages that pair off with the rank 
        distance numRanks/2 away
        """

        self.setPairedMessagesByDistance(job, job.numRanks/2)

    def setPairedMessagesByDistance(self, job, distance):
        """ 
        Create a group of messages that pair off with the rank the provided
        distance away
        """

        messages = []
        hosts = job.getHosts()
        numBlocks = job.numRanks/(2*distance)
        blockIndex = 0
        for block in range(numBlocks):
            for rank in range(distance):
                rank1 = blockIndex+rank
                rank2 = blockIndex+rank+distance
                message = Message(rank1, rank2, hosts[rank1], hosts[rank2]) 
                messages.append(message)
            blockIndex = blockIndex + distance*2
        self.messages = messages

    def setOneToAllMessages(self, job):
        """
        Set rank 0 to communicate with all other ranks. 
        Probably don't actually want this as currently it will be treated as sending to
        all ranks simultaneously
        """

        messages = []
        hosts = job.getHosts()
        for rank in job.getRanks():
            message = Message(0, rank, hosts[0], hosts[rank]) 
            messages.append(message)

        self.messages = messages


    def setBandwidths(self, cluster):
        """
        Given a group of messages and a model for the maximum point to point and
        inter-island bandwidth, calculate the bandwidth that each message could
        be sent at, assuming all those messages were sent simultaneously
        """
        islandCounts = np.zeros(cluster.numIslands, dtype=int)

        # calculate total number of ranks which need to use each edge switch 
        # (ie how many ranks in each island communicate with a different island)
        for message in self.messages:
            island1 = int(floor(message.host1/float(cluster.nodesPerIsland)))
            island2 = int(floor(message.host2/float(cluster.nodesPerIsland)))
            message.setIslands(island1, island2)
           
            if (island1 != island2):
                islandCounts[island1] = islandCounts[island1]+1
                islandCounts[island2] = islandCounts[island2]+1

        # set bandwidths based on the saturation of the islands in the network
        for message in self.messages:
            if (message.island1 != message.island2):
                sharedIslandBw1 = cluster.islandBandwidth/float(islandCounts[message.island1])
                bw1 = min(cluster.singleNodeBandwidth, sharedIslandBw1)
                sharedIslandBw2 = cluster.islandBandwidth/float(islandCounts[message.island2])
                bw2 = min(cluster.singleNodeBandwidth, sharedIslandBw2)

                message.bandwidth = min(bw1, bw2)
            else:
                message.bandwidth = cluster.singleNodeBandwidth


    def __str__(self):
        fullString = ""
        count = 0
        for message in self.messages:
            fullString = fullString + "message " + str(count) + ": " + str(message) +  "\n"
            count = count + 1
        return fullString

"""
Object representing a job to run on a cluster. Takes a number of ranks and allocates them
to host IDs using different allocation strategies:
    allocateScattered
    allocateSplitIslands
    allocateRandom
"""
class Job:
    def __init__(self, numRanks=8):
        self.numRanks = numRanks

    def allocatePacked(self, cluster):
        """ 
        Allocate consecutive ranks to consecutive host ids. 
        Host IDs are integers [0:numIslands*nodesPerIsland] 
        """

        hosts = range(self.numRanks)
        self.hosts = hosts

    def allocateScattered(self, cluster):
        """ 
        Allocate ranks spaced by the maximum possible regular interval, starting at host 0.
        If the interval is one, pack from the left.
        Host IDs are integers [0:numIslands*nodesPerIsland] 
        """

        hosts = []
        interval = int(ceil(cluster.numNodes/self.numRanks))
	for i in range(self.numRanks):
	    if i*interval < cluster.numNodes:
		hosts.append(i*interval)
	    else:
		hosts.append(hosts[i-1]+1)
        self.hosts = hosts

    def allocateSplitIslands(self, cluster):
        """
        Allocate ranks to hosts by dividing ranks evenly between islands. 
        Rank order is maintained and ranks are packed within an island.
        The last island may contain fewer ranks if they don't split evenly.
        Host IDs are integers [0:numIslands*nodesPerIsland] 
        """

        hosts = []
        numRanksPerIsland = int(ceil(self.numRanks/cluster.numIslands))
	for i in range(cluster.numIslands):
	    for r in range(numRanksPerIsland):
		host = i*cluster.nodesPerIsland + r
		if host < cluster.numNodes:
		    hosts.append(host)
        self.hosts = hosts

    def allocateRandom(self, cluster): 
        """ 
        Allocate ranks to hosts maintaining rank order but spaced at random intervals.
        Host IDs are integers [0:numIslands*nodesPerIsland] 
        """

        allHosts = range(cluster.numNodes)
        shuffle(allHosts)
        hosts = allHosts[0:self.numRanks]
        hosts.sort()

        self.hosts = hosts

    def getHosts(self):
        return self.hosts

    def getRanks(self):
        return range(self.numRanks)

    def __str__(self):
        jString = "Job: " + str(self.numRanks) + " ranks on hosts " + str(self.getHosts())
        return jString



import heapq

def merge_k_sorted_lists(lists, get_value):
    output = []
    pq = []

    for index, list_to_merge in enumerate(lists):
        heapq.heappush(pq, (get_value(list_to_merge[0]), (index, 0)))

    while(len(pq) != 0):
        curr = heapq.heappop(pq)

        i = curr[1][0]
        j = curr[1][1]

        output.append(lists[i][j])

        if ((j+1) < len(lists[i])):
            heapq.heappush(pq,(get_value(lists[i][j+1]),(i,j+1)))

    return output

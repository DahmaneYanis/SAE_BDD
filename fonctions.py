def doubleTri_selection(tabMaster, tabSlave):
    n = len(tabMaster)
    for i in range(n - 1):
        min_index = i
        for j in range(i + 1, n):
            if tabMaster[j] < tabMaster[min_index]:
                min_index = j
        tabMaster[i], tabMaster[min_index] = tabMaster[min_index], tabMaster[i]
        tabSlave[i], tabSlave[min_index] = tabSlave[min_index], tabSlave[i]


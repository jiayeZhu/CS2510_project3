import numpy as np 
from sklearn.neighbors import NearestNeighbors as KNN


f = open('input/23p.csv','r')
lines = f.readlines()

def calD(x1,y1,x2,y2):
    return ((x1-x2)**2+(y1-y2)**2)**0.5


pArray = []
for line in lines:
    parts = line.split(',')
    pArray.append([float(parts[1]),float(parts[2])])

X = np.array(pArray)
nbrs = KNN(n_neighbors=4, algorithm='ball_tree').fit(X)
distances, indices = nbrs.kneighbors(X)
print(distances)
print(indices)

#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pprint import pprint


# In[5]:


from py2neo import Graph


# In[6]:


g = Graph("http://localhost:7474", password = "root")


# In[7]:


query = """
MATCH (cat:Category)<-[g:OF_GENRE]-()
RETURN cat.Name AS genre, COUNT(g) AS number_of_movies
ORDER BY number_of_movies DESC;
"""


# In[8]:


g.run(query).to_data_frame()


# In[9]:


query = """
        // get target user and their neighbors pairs and count 
        // of distinct movies that they have rented in common
        MATCH (c1:Customer)-[:RENTED]->(f:Film)<-[:RENTED]-(c2:Customer)
        WHERE c1 <> c2 AND c1.customerID = $cid
        WITH c1, c2, COUNT(DISTINCT f) as intersection

        // get count of all the distinct movies that they have rented in total (Union)
        MATCH (c:Customer)-[:RENTED]->(f:Film)
        WHERE c in [c1, c2]
        WITH c1, c2, intersection, COUNT(DISTINCT f) as union

        // compute Jaccard index
        WITH c1, c2, intersection, union, (intersection * 1.0 / union) as jaccard_index
        
        // get top k nearest neighbors based on Jaccard index
        ORDER BY jaccard_index DESC, c2.customerID
        WITH c1, COLLECT([c2.customerID, jaccard_index, intersection, union])[0..$k] as neighbors
     
        WHERE SIZE(neighbors) = $k   // return users with enough neighbors
        RETURN c1.customerID as customer, neighbors
        """

neighbors = {}
for i in g.run(query, cid = "13", k = 25).data():
    neighbors[i["customer"]] = i["neighbors"]

print("# customer13's 25 nearest neighbors: customerID, jaccard_index, intersection, union")
pprint(neighbors)


# In[10]:


nearest_neighbors = [neighbors["13"][i][0] for i in range(len(neighbors["13"]))]

query = """
        // get top n recommendations for customer 13 from their nearest neighbors
        MATCH (c1:Customer),
              (neighbor:Customer)-[:RENTED]->(f:Film)    // all movies rented by neighbors
        WHERE c1.customerID = $cid
          AND neighbor.customerID in $nearest_neighbors
          AND not (c1)-[:RENTED]->(f)                    // filter for movies that our user hasn't rented
        
        WITH c1, f, COUNT(DISTINCT neighbor) as countnns // times rented by nns
        ORDER BY c1.customerID, countnns DESC               
        RETURN c1.customerID as customer, COLLECT([f.Title, countnns])[0..$n] as recommendations  
        """

recommendations = {}
for i in g.run(query, cid = "13", nearest_neighbors = nearest_neighbors, n = 5).data():
    recommendations[i["customer"]] = i["recommendations"]
    
print("# customer13's recommendations: Movie, number of rentals by neighbors")
pprint(recommendations)


# In[11]:


import sys


# In[12]:


from pprint import pprint
from py2neo import Graph


# In[15]:


cid = sys.argv[1:]


# In[17]:


g = Graph("http://localhost:7474", password = "root")

def cf_recommender(graph, cid, nearest_neighbors, num_recommendations):

    query = """
           MATCH (c1:Customer)-[:RENTED]->(f:Film)<-[:RENTED]-(c2:Customer)
           WHERE c1 <> c2 AND c1.customerID = $cid
           WITH c1, c2, COUNT(DISTINCT f) as intersection
           
           MATCH (c:Customer)-[:RENTED]->(f:Film)
           WHERE c in [c1, c2]
           WITH c1, c2, intersection, COUNT(DISTINCT f) as union

           WITH c1, c2, intersection, union, 
              (intersection * 1.0 / union) as jaccard_index

           ORDER BY jaccard_index DESC, c2.customerID
           WITH c1, COLLECT(c2)[0..$k] as neighbors
           WHERE SIZE(neighbors) = $k                                              
           UNWIND neighbors as neighbor
           WITH c1, neighbor

           MATCH (neighbor)-[:RENTED]->(f:Film)         
           WHERE not (c1)-[:RENTED]->(f)                        
           WITH c1, f, COUNT(DISTINCT neighbor) as countnns
           ORDER BY c1.customerID, countnns DESC                            
           RETURN c1.customerID as customer, 
              COLLECT(f.Title)[0..$n] as recommendations      
           """

    recommendations = {}
    cid = [str(c) for c in cid]
    for c in cid:
        for i in graph.run(query, cid = c, k = nearest_neighbors, n = num_recommendations).data():
            recommendations[i["customer"]] = i["recommendations"]
    return recommendations

pprint(cf_recommender(g, cid, 25, 5))


# In[ ]:





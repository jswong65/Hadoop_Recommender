# Hadoop_Recommender
Item collaborative filtering implementation with Hadoop

Typically, we take row(i) from a matrix A (R<sup>m-by-k</sup>), and a column(j) from a matrix B (R<sup>k-by-n</sup>) to perform a dot product to get the value of the cell (i, j) in the output matrix C (R<sup>m-by-n</sup>). However, when we need to multiply two matrixes with a large size, we may not be able to fit the entire row or entire columns into memory. Therefore, when performing matrix multiplication using Hadoop, we will need one MapReduce process to multiply two matrix cell by cell with a proper key assignment as an intermediate result of each cell of the new matrix. Another MapReduce process can then aggregate the corresponding values for each cell.

To obtain the movie recommendation, we need: 
* A co-occurrence matrix that specifies how many users watched both movie A and also and movie B. Here, we made an assumption that if movie A and movie B were both watched by many users, those movies are considered relevant to each other. Commonly, if a user likes action movies, s/he tends to watch more action movies than other types of movies. Therefore, the values of the co-occurrence matrix implies the similarity between two movies. Additionally, we need to normalize the the co-occurrence matrix  
* A user rating matrix that indicates the ratings to movies given by users, which can be useful to understand the preference of a user.

Data: Netflix Movie Review
Input Format: UserId, MovieId, Rating

### MapReduce components:
1. [DataDividerByUser](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/DataDividerByUser.java): Concatenates all of the rating given by individual users.
  * 
2. [CoOccurrenceMatrixGenerator](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/CoOccurrenceMatrixGenerator.java): Creates a co-occurrence matrix based on the frequency of two movies rated by an user. 
3. [Normalize](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/Normalize.java): Normalizes the co-occurrence by row as the similarity score between two movies. 
4. [Multiplication](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/Multiplication.java):
5. [Sum](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/Sum.java): Sums up the associated Multiplication of a movie.

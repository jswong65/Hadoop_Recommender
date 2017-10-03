# Hadoop Recommender
Item collaborative filtering implementation with Hadoop

Typically, we take row(i) from a matrix A (R<sup>mk</sup>), and a column(j) from a matrix B (R<sup>kn</sup>) to perform a dot product to get the value of the cell (i, j) in the output matrix C (R<sup>mn</sup>). However, when we need to multiply two matrixes with a large size, we may not be able to fit the entire row or entire columns into memory. Therefore, when performing matrix multiplication using Hadoop, we will need one MapReduce process to multiply two matrix cell by cell with a proper key assignment as an intermediate result of each cell of the new matrix. Another MapReduce process can then aggregate the corresponding values for each cell.

To obtain the movie recommendation, we need: 
* A co-occurrence matrix that specifies how many users watched both movie A and also and movie B. Here, we made an assumption that if movie A and movie B were both watched by many users, those movies are considered relevant to each other. Commonly, if a user likes action movies, s/he tends to watch more action movies than other types of movies. Therefore, the values of the co-occurrence matrix implies the similarity between two movies. Additionally, we need to normalize the the co-occurrence matrix  
* A user rating matrix that indicates the ratings to movies given by users, which can be useful to understand the preference of a user.

Data: Netflix Movie Review
Input Format: UserId, MovieId, Rating

### MapReduce components:
1. [DataDividerByUser](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/DataDividerByUser.java): Concatenates all of the rating given by individual users.
    * **Input**: UserId,MovieId,Rating
    * **DataDividerMapper**: Creates the <UserId, MovieId:Rating> pairs
    * **DataDividerReducer**: Write out the list of movies rated by a user <UserId, Movie1:Rating, Movie2:Rating, ...>
2. [CoOccurrenceMatrixGenerator](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/CoOccurrenceMatrixGenerator.java): Creates a co-occurrence matrix based on the frequency of two movies rated by an user. 
    * **Input**: UserId\t Movie1:Rating,Movie2:Rating, ...
    * **MatrixGeneratorMapper**: Creates the <Movie1=Movie2, 1> pairs
    * **MatrixGeneratorReducer**: Writes out the aggregated co-occurrence frequency of between pairs of moves. <Movie1=Movie2, occurrence>
3. [Normalize](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/Normalize.java): Normalizes the co-occurrence by row as the similarity score between two movies. 
    * **Input**: Movie1=Movie2\t occurrence
    * **NormalizeMapper** Creates the <Movie1, Movie2=occurrence>
    * **NormalizeReducer** Writes out the <Movie2, Movie1=normalized_occurrence>. 
        * The reason why we have Movie2 as key is that we need such information to generate a unit of matrix multiplication. 
        * **How I would interpret it** - we can consider that the <Movie2, Movie1=normalized_occurrence> pairs store the weight information how Movie2 can contribute the user rating to Movie1. Assume User1 give Movie2 a rating 10:
            * if we have the occurrence pair <Movie2, Movie1=0.8>, then Movie 2 would contribute 10 * 0.8 to the rating of Movie1 that User 1 might give. 
            * if we have the occurrence pair <Movie2, Movie1=0.2>, then Movie 2 would contribute 10 * 0.2 to the rating of Movie1 that User 1 might give.
        * **conclusion**: if the normalized_occurrence is larger, it may imply Movie2 could have more impact on the recommendation of Movie1. 
4. [Multiplication](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/Multiplication.java):
    * **Input1**: 
    * **CooccurrenceMapper**:
    * **Input2**:
    * **RatingMapper**:
    * **MultiplicationReducer**:
5. [Sum](https://github.com/jswong65/Hadoop_Recommender/blob/master/src/main/java/Sum.java): Sums up the associated Multiplication of a movie.

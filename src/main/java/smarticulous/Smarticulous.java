package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Exercise.Question;
import smarticulous.db.Submission;
import smarticulous.db.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The Smarticulous class, implementing a grading system.
 */
public class Smarticulous {

    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the {@link Smarticulous} SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables, creating them if necessary:
     *
     * <table>
     *   <caption><em>Table name: <strong>User</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>UserId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Username</td><td>Text</td></tr>
     *   <tr><td>Firstname</td><td>Text</td></tr>
     *   <tr><td>Lastname</td><td>Text</td></tr>
     *   <tr><td>Password</td><td>Text</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Exercise</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>DueDate</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Question</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>Desc</td><td>Text</td></tr>
     *   <tr><td>Points</td><td>Integer</td></tr>
     * </table>
     * In this table the combination of ExerciseId and QuestionId together comprise the primary key.
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Submission</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>UserId</td><td>Integer</td></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>SubmissionTime</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>QuestionGrade</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Grade</td><td>Real</td></tr>
     * </table>
     * In this table the combination of SubmissionId and QuestionId together comprise the primary key.
     *
     * @param dburl The JDBC url of the database to open (will be of the form "jdbc:sqlite:...")
     * @return the new connection
     * @throws SQLException
     */
    
    public Connection openDB(String dburl) throws SQLException {
        
        // Open the SQLite database
        db =  DriverManager.getConnection(dburl);
        
        // Ensure the necessary tables exist
        try (  Statement st = db.createStatement() ) {
            
            // Create User table
            st.executeUpdate("CREATE TABLE IF NOT EXISTS User (UserId INTEGER PRIMARY KEY, Username TEXT UNIQUE, Firstname TEXT, Lastname TEXT, Password TEXT)");
            
            // Create Exercise table
            st.executeUpdate("CREATE TABLE IF NOT EXISTS Exercise (ExerciseId INTEGER PRIMARY KEY, Name TEXT, DueDate INTEGER)");

            // Create Question table
            st.executeUpdate("CREATE TABLE IF NOT EXISTS Question (ExerciseId INTEGER, QuestionId INTEGER, Name TEXT, Desc TEXT, Points INTEGER, PRIMARY KEY (ExerciseId, QuestionId))");

            // Create Submission table
            st.executeUpdate("CREATE TABLE IF NOT EXISTS Submission (SubmissionId INTEGER PRIMARY KEY, UserId INTEGER, ExerciseId INTEGER, SubmissionTime INTEGER)");

            // Create QuestionGrade table
            st.executeUpdate("CREATE TABLE IF NOT EXISTS QuestionGrade (SubmissionId INTEGER, QuestionId INTEGER, Grade REAL, PRIMARY KEY (SubmissionId, QuestionId))");

        } catch (SQLException e) {

            e.printStackTrace();
        }

        return db;
    }


    /**
     * Close the DB if it is open.
     *
     * @throws SQLException
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist. If a user with user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @param password
     * @return the userid.
     * @throws SQLException
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        
        String check_existance = "SELECT UserId FROM User WHERE Username = ?";
        String change_values = "UPDATE User SET Firstname = ?, Lastname = ?, Password = ? WHERE Username = ?";
        String create_values = "INSERT INTO User (Username,Firstname,Lastname,Password) VALUES (?,?,?,?)"; 

        try ( PreparedStatement st = db.prepareStatement(check_existance) ) {
            st.setString(1, user.username);
            ResultSet res = st.executeQuery();

            if ( res.next() ) {
                // Username exists
                try ( PreparedStatement st1 = db.prepareStatement(change_values) ) {
                    st1.setString(1, user.firstname.trim());
                    st1.setString(2, user.lastname.trim());
                    st1.setString(3, password.trim());
                    st1.setString(4, user.username.trim());

                    if ( st1.executeUpdate() != 0 ) {
                        return res.getInt("UserId");
                    }
                }
            }
            else {
                try ( PreparedStatement st2 = db.prepareStatement(create_values,Statement.RETURN_GENERATED_KEYS) ) {
                    st2.setString(1, user.username.trim());
                    st2.setString(2, user.firstname.trim());
                    st2.setString(3, user.lastname.trim());
                    st2.setString(4, password.trim());

                    if ( st2.executeUpdate() != 0 ) {
                        ResultSet res1 = st2.getGeneratedKeys();
                        return res1.getInt(1);
                    }
                }
            }
        } catch (SQLException e) {

            e.printStackTrace();
        }
        return -1;
    }


    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches; false otherwise.
     * @throws SQLException
     * <p>
     * Note: this is totally insecure. For real-life password checking, it's important to store only
     * a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        
        String check_existance = "SELECT Password FROM User WHERE Username = ?";

        try ( PreparedStatement st = db.prepareStatement(check_existance) ) {
            st.setString(1, username.trim());
            ResultSet res = st.executeQuery();

            if ( res.next() ) {
                // Username exists
                String user_password = res.getString("Password");
                return password.equals(user_password);
            }
        } catch (SQLException e) {

            e.printStackTrace();
        }

        return false;
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     * @throws SQLException
     */
    public int addExercise(Exercise exercise) throws SQLException {
        
        String check_existance = "SELECT ExerciseId FROM Exercise WHERE ExerciseId = ?";
        String add_values = "INSERT INTO Exercise (ExerciseId, Name, DueDate) VALUES (?, ?, ?)";
        String add_question = "INSERT INTO Question (ExerciseId, QuestionId, Name, Desc, Points) VALUES (?, ?, ?, ?, ?)";

        try ( PreparedStatement st = db.prepareStatement(check_existance) ) {
            st.setInt(1,exercise.id);
            ResultSet res = st.executeQuery();

            if (res.next()) {
                return -1;
            }
            
            try ( PreparedStatement st1 = db.prepareStatement(add_values, PreparedStatement.RETURN_GENERATED_KEYS)) {
                st1.setInt(1,exercise.id);
                st1.setString(2, exercise.name.trim());
                st1.setDate(3, new java.sql.Date(exercise.dueDate.getTime()));
                
                int change = st1.executeUpdate();
                

            if ( change != 0 ) {
            
            try (ResultSet generatedKeys = st1.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                   int exercise_id = generatedKeys.getInt(1);
                   
                   for (int i = 0; i < exercise.questions.size(); i++ ) {

                   Exercise.Question question = exercise.questions.get(i);

                        try (PreparedStatement st2 = db.prepareStatement(add_question)) {
                            st2.setInt(1,exercise.id);
                            st2.setInt(2,i + 1);
                            st2.setString(3, question.name.trim());
                            st2.setString(4, question.desc.trim());
                            st2.setInt(5, question.points);
                            st2.executeUpdate();
                        }
                   }
                   return exercise_id;
                }
            }
        }
               }     

        }  catch (SQLException e) {

            e.printStackTrace();
        }
        return -1;
    }
    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     *
     * @return list of all exercises.
     * @throws SQLException
     */
    public List<Exercise> loadExercises() throws SQLException {
        
        String extract_exercise = "SELECT * FROM Exercise";
        String questions_select = "SELECT * FROM Question WHERE ExerciseId = ?";
        List<Exercise> exercises = new ArrayList<>();

        try ( PreparedStatement st = db.prepareStatement(extract_exercise) ) {
            ResultSet res = st.executeQuery();

            while ( res.next() ) {
                int ex_Id = res.getInt("ExerciseId");
                String ex_Name = res.getString("Name");
                Date ex_DueDate = res.getDate("DueDate");

                Exercise exercise = new Exercise(ex_Id, ex_Name, ex_DueDate);

                exercises.add(exercise);
            
                try ( PreparedStatement st1 = db.prepareStatement(questions_select) ) {
                    st1.setInt(1,exercise.id);
                    ResultSet res1 = st1.executeQuery();

                    while ( res1.next() ) {
                        String qs_Name = res1.getString("Name");
                        String qs_Desc = res1.getString("Desc");
                        int qs_Points = res1.getInt("Points");

                        exercise.addQuestion(qs_Name, qs_Desc, qs_Points);

                    }
                }

            }

        }

        return exercises;
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     * @throws SQLException
     */
    public int storeSubmission(Submission submission) throws SQLException {
        
        String check_userExists = "SELECT * FROM User WHERE Username = ?";
        String add_values = "INSERT INTO Submission (UserId, ExerciseId, SubmissionTime) VALUES (?,?,?)";
        int userId;

        try ( PreparedStatement st = db.prepareStatement(check_userExists) ) {
            st.setString(1,submission.user.username);
            ResultSet res = st.executeQuery();
            userId = res.getInt("UserId");

            if (!res.next()) { // User doesnt exists 
                return -1;
            }

        }
            
            // User exists
            
            try ( PreparedStatement st1 = db.prepareStatement(add_values)) {

                    st1.setInt(1,userId);
                    if (submission.exercise.id == -1) {
                        return -1;
                    }
                    st1.setInt(2,submission.exercise.id);
                    st1.setDate(3,new java.sql.Date(submission.submissionTime.getTime()));
                    st1.executeUpdate();
                    ResultSet res1 = st1.getGeneratedKeys();
                    if ( res1.next() ) {
                        return res1.getInt(1);
                    }

            }catch (SQLException e) {

                e.printStackTrace();
            }
                
            return -1;               
        }
        





    


    // ============= Submission Query ===============


    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
        
        String result = "SELECT Submission.SubmissionId, QuestionGrade.QuestionId, QuestionGrade.Grade, Submission.SubmissionTime FROM Submission JOIN QuestionGrade ON Submission.SubmissionId = QuestionGrade.SubmissionId WHERE Submission.SubmissionTime = (SELECT MAX(Submission.SubmissionTime) FROM Submission JOIN User ON Submission.UserId = User.UserId WHERE User.Username = ? AND Submission.ExerciseId = ?) ORDER BY QuestionGrade.QuestionId LIMIT ? ";
        PreparedStatement stmt = db.prepareStatement(result);

        return stmt;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     *
     */
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @param stmt
     * @return
     * @throws SQLException
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     * @throws SQLException
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }


    /**
     * Return the submission with the highest total grade
     *
     * @param user the user for which we retrieve the best submission
     * @param exercise the exercise for which we retrieve the best submission
     * @return
     * @throws SQLException
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}

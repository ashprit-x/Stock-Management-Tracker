/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2409755
 *
 */

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import java.io.*;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
    //Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
    //these clasess are not exported by the module. Instead, one needs to impor
    //javax.sql.rowset.* as above.



public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome   = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){
        
		//TO BE COMPLETED
        serviceSocket = aSocket;
        this.start();
		
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop
		
		String tmp = "";
        try {
            InputStream socketStream = this.serviceSocket.getInputStream();
            InputStreamReader socketReader = new InputStreamReader(socketStream);
            StringBuffer stringBuffer = new StringBuffer(); // threadsafe
            char x;
            while (true) //Read until terminator character is found
            {
                System.out.println("Service thread: reading characters ");
                x = (char) socketReader.read();
                System.out.println("Service thread: " + x);
                if (x == '#')
                    break;
                stringBuffer.append(x);
            }
            String temp = stringBuffer.toString();
            this.requestStr[0] = temp.substring(0,temp.indexOf(';'));
            this.requestStr[1] = temp.substring(temp.indexOf(';')+1);
			
         }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		
		this.outcome = null;
		
		String sql = "SELECT r.title, label, genre, rrp, count(copyid) as copyID, recordshop.name from artist left join public.record r on artist.artistid = r.artistid left join public.recordcopy r2 on r.recordid = r2.recordid left join public.genre g on r.genre = g.name left join public.recordshop on r2.recordshopid = recordshop.recordshopid WHERE city = ? AND artist.lastname = ? group by r.title, label, genre, rrp, recordshop.name;"; //TO1 BE COMPLETED- Update this line as needed.

		Connection connection = null;
        PreparedStatement statement = null;
		
		try {
			//Connet to the database
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);

			//TO BE COMPLETED
            statement = connection.prepareStatement(sql);
            statement.setString(1, requestStr[1]);
            statement.setString(2, requestStr[0]);
//            statement.executeUpdate();

			//Make the query
            outcome = statement.executeQuery();
			//TO BE COMPLETED
			
			//Process query
            RowSetFactory rowSetFactory = RowSetProvider.newFactory();
            CachedRowSet crs = rowSetFactory.createCachedRowSet();
            crs.populate(outcome);
            outcome = crs;


			//TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.

			//Clean up
            connection.close();
            statement.close();

			//TO BE COMPLETED
			
		} catch (Exception e)
		{ System.out.println(e); }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
			//Return outcome
			//TO BE COMPLETED
            OutputStream outcomeStream = this.serviceSocket.getOutputStream();
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(outcomeStream);
            outcomeStreamWriter.writeObject(this.outcome); //Wrap and add termination character
            outcomeStreamWriter.flush();
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

			//Terminating connection of the service socket
			//TO BE COMPLETED
            this.serviceSocket.close();
			
        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}

package source;
import java.io.Console;

import java.sql.*;
import java.util.ArrayList;

import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.*;
import com.google.gson.Gson;


public class Serverconnection implements AutoCloseable {

    private Connection connect;
    private Statement stmt;

    private Console console = System.console();

    private Server server = new Server();
    private Gson converter = new Gson();
    private Logger logger ;

    public Serverconnection (Server server) throws Exception {
        logger = GlobalLogger.getLogger();
        String url = "jdbc:sqlserver://" + server.host + ";instanceName="+ server.instance + ";databaseName=" + server.dbName +";trustServerCertificate=true";
        try {
            this.connect = DriverManager.getConnection(url, server.user, server.password); 
        } catch (Exception e) {
            logger.severe(e.toString());
        }
        stmt = connect.createStatement();
        System.out.println("connessione riuscita a " + server.host + "\\\\" + server.instance );
        logger.fine("connessione riuscita a " + server.host + "\\\\" + server.instance );
    }

    public Serverconnection (ArrayList <Server> connessioni) throws Exception{
        this.server.nome =  console.readLine("nome: ");
        this.server.host =  console.readLine("Host: ");
        this.server.instance =  console.readLine("Istanza: ");
        this.server.dbName =  console.readLine("database: ");

        this.server.user =  console.readLine("user: ");
        this.server.password =  new String(console.readPassword("password: "));
        

        String url = "jdbc:sqlserver://" + server.host + ";instanceName="+ server.instance + ";databaseName=" + server.dbName +";trustServerCertificate=true";
        System.out.println(); 
    
        try {
            connect = DriverManager.getConnection(url, server.user, server.password); 
        } catch (Exception e) {
            logger.severe(e.toString());
        }
        
        stmt = connect.createStatement();
        System.out.println("connessione riuscita a " + server.host + "\\\\" + server.instance + "\n\n");

        while(true){
            String response = console.readLine("Si desidera salvare la connessione? [y/n]");

            if(response.equals("y")){
                connessioni.add(server);
                String json = converter.toJson(connessioni);
                
                String path = "./res/connectios.json";

                try (FileWriter writer = new FileWriter(path)) {
                    writer.write(json);
                    System.out.println("Salvato con successo in " + path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
                    } else if (response.equals("n")) {
                break;
            }
        }   
    } 

    public ResultSet query (String queryRequest) throws Exception{
        return stmt.executeQuery(queryRequest);
    }

    public  Connection getConnection () {
        return connect;
    }

    @Override
    public void close() {
        System.out.println("Connessione al database chiusa");
        logger.fine("closed connections");
        
    }

    public void update (Serverconnection source, Serverconnection terget) throws Exception{

    }
}
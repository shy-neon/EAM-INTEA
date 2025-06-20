package source;
import java.io.Console;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;


public class Main {
    public static void main(String[] args) {
  
        Gson parser = new Gson();
        
        clearOutput();
        ArrayList <Server> listaConnessioni = new ArrayList<>();
        try {
            String content = Files.readString(Paths.get("./res/connectios.json"));
            Type listType = new TypeToken<ArrayList<Server>>(){}.getType();
            listaConnessioni = parser.fromJson(content, listType);
        } catch (IOException e) {
            e.printStackTrace();
        }

            while(true){
                System.out.println("*** EAM INTEA DATABASE SYNC UTILITY ***");
                System.out.println("1] Nuova connessione \n2] List connessioni \n3] Reset List \n4] Execute");
                Scanner scan = new Scanner(System.in);
                int scelta = scan.nextInt();

                switch (scelta) {
                case 1:
                    addConnection(listaConnessioni);
                    break;
                case 2:
                    clearOutput();
                    for(Server server : listaConnessioni){
                        server.printServer();
                    }
                    break;
                case 3:
                    deletConnectionsRecord();
                    break;
                case 4:
                    execute(listaConnessioni.get(1), listaConnessioni.get(0));
                break;
                default:
                    break;
                }
        
            }
        }

    static void clearOutput () {
        for (int i = 0; i < 50; i++) 
        System.out.println(); 
    }

    static void execute (Server target, Server source) {
         try {
            Serverconnection trg = new Serverconnection(target); 
            Serverconnection src = new Serverconnection(source);
           
            
            ResultSet res1 = src.query("SELECT * FROM EAM2GIS_OGGETTI");
            
            PreparedStatement ps = trg.getConnection().prepareStatement("INSERT INTO CONTATORI (OBJECTID, COMUNE, VIA_DENOMINAZIONE, DATA_INS, DATA_AGG, POINT_X, POINT_Y) VALUES (?,?,?,?,?,?,?)");
            int i = 1;

            while(res1.next()){
                ps.setInt(1, 3532+ i);
                ps.setString(2, res1.getString("E2G_COMUNE"));
                ps.setString(3, res1.getString("E2G_INDIRIZZO"));
                ps.setString(4, res1.getString("E2G_DATA_CREAZIONE"));
                ps.setString(5, res1.getString("E2G_DATA_MODIFICA"));
                ps.setString(6, res1.getString("E2G_COOX"));
                ps.setString(7, res1.getString("E2G_COOY"));
                ps.executeUpdate();
                System.out.println(res1.getInt("E2G_COMUNE"));
                i++;
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }   

    static void deletConnectionsRecord() {
        Console console = System.console();
        String response = console.readLine("Si desidera eliminare il record delle connessioni? [y/n]");
        
        while(true){
            if(response.equals("y")){
                String path = "./res/connectios.json";
                try (FileWriter writer = new FileWriter(path)) {
                    writer.write("[]");
                    System.out.println("eliminati con successo con successo in " + path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                console.readLine("Elementi elimintai con successo any key to continue");
                break;
            } else  {
                break;
            }    
        }
        clearOutput();
    }

    static void addConnection(ArrayList<Server> listaConnessioni) {
        clearOutput();
        try{
            Serverconnection connection = new Serverconnection(listaConnessioni); //il costruttore di Serverconnection si occupa di scrivere il file su json
            connection.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

  
}


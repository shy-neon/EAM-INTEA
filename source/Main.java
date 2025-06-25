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

        ArrayList <Server> listaConnessioni = new ArrayList<>();
        ArrayList <Direction> directions = new ArrayList<>();
        
        Gson parser = new Gson();
        listaConnessioni = updateListFromJson(listaConnessioni, parser);
        directions = updateListDirFromJson(directions, parser);

        clearOutput();

            if(args.length != 0){
                clone(listaConnessioni.get(1), listaConnessioni.get(0));
            }

            while(true && args.length == 0){
                System.out.println("*** EAM INTEA DATABASE SYNC UTILITY ***");
                System.out.println("1] Nuova connessione        2] List connessioni \n3] Reset List               4] Copia \n5] Update                   6] Add direction\n7] Direction List");
                Scanner scan = new Scanner(System.in);
                int scelta = scan.nextInt();

                switch (scelta) {
                case 1:
                    addConnection(listaConnessioni);
                    break;
                case 2:
                    clearOutput();
                    try {
                        String content = Files.readString(Paths.get("./res/connectios.json"));
                        Type listType = new TypeToken<ArrayList<Server>>(){}.getType();
                        listaConnessioni = parser.fromJson(content, listType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    for(Server server : listaConnessioni){
                        server.printServer();
                    }
                    break;
                case 3:
                    deletConnectionsRecord(listaConnessioni);
                    break;
                case 4:
                    clone(listaConnessioni.get(1), listaConnessioni.get(0));
                    break;
                case 5:
                    update(listaConnessioni.get(1), listaConnessioni.get(0));
                    break;
                case 6:
                    addDirection(listaConnessioni, directions, parser);
                    break;
                case 7:
                   clearOutput();
                    try {
                        String content = Files.readString(Paths.get("./res/exec.json"));
                        Type listType = new TypeToken<ArrayList<Direction>>(){}.getType();
                        directions = parser.fromJson(content, listType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    for(Direction dir : directions){
                        dir.printDirection();
                    }
                    break;
                default:
                    break;
                }
        
            }
        }

    static ArrayList<Direction> addDirection (ArrayList<Server> listaConnessioni, ArrayList<Direction> direzioni, Gson parser) {

        clearOutput();

        Console console = System.console();
        Gson converter = new Gson();
        String path = "./res/exec.json";

        String type = console.readLine("update(0)/clone(1): ");
        String source = console.readLine("server di source: ");
        String target = console.readLine("server di target: ");
        String tabella = console.readLine("tabella da clonare: ");
        Direction dir = new Direction(type, source, target, listaConnessioni, tabella);

        direzioni.add(dir);
        String json = converter.toJson(direzioni);
        try (FileWriter writer = new FileWriter(path)) {
            writer.write(json);
            System.out.println("Salvato con successo in " + path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return direzioni;
    
    }
    static void clearOutput () {
        for (int i = 0; i < 50; i++) 
        System.out.println(); 
    }

    static void clone (Server intea, Server acoset) {
         try {
            Serverconnection inte = new Serverconnection(intea); 
            Serverconnection acos = new Serverconnection(acoset);
           
            ResultSet res1 = inte.query("SELECT * FROM CONTATORI");
            int objid = 0;

            res1 = acos.query("SELECT * FROM EAM2GIS_OGGETTI");
            PreparedStatement ps = inte.getConnection().prepareStatement ("DELETE FROM CONTATORI");
            ps.executeUpdate();

            ps = inte.getConnection().prepareStatement("INSERT INTO CONTATORI (OBJECTID, COMUNE, VIA_DENOMINAZIONE, DATA_INS, DATA_AGG, D_STATO, POINT_X, POINT_Y) VALUES (?,?,?,?,?,?,?,?)");
            
            while(res1.next()){
                ps.setInt(1, objid);
                ps.setString(2, res1.getString("E2G_COMUNE"));
                ps.setString(3, res1.getString("E2G_INDIRIZZO"));
                ps.setString(4, res1.getString("E2G_DATA_CREAZIONE"));
                ps.setString(5, res1.getString("E2G_DATA_MODIFICA"));
                ps.setString(6, res1.getString("E2G_STATUS"));
                ps.setString(7, res1.getString("E2G_COOX"));
                ps.setString(8, res1.getString("E2G_COOY"));
                ps.executeUpdate(); 
                objid++;
            }

            CloneAgent cloneSERB = new CloneAgent(inte, acos, "acq_serbatoio");
            cloneSERB.cloneTable();

            CloneAgent cloneCOND = new CloneAgent(inte, acos, "acq_condotta");
            cloneCOND.cloneTable();

            CloneAgent clonePOZZ = new CloneAgent(inte, acos, "acq_pozzo");
            clonePOZZ.cloneTable();

            CloneAgent cloneCAM = new CloneAgent(inte, acos, "acq_cameretta");
            cloneCAM.cloneTable();

            inte.close();
            acos.close();

        } catch (Exception e) {
            System.err.println(e);
        }
    }   

    static ArrayList<Server> updateListFromJson (ArrayList<Server> listaConnessioni, Gson parser) {
        try {
            String content = Files.readString(Paths.get("./res/connectios.json"));
            Type listType = new TypeToken<ArrayList<Server>>(){}.getType();
            listaConnessioni = parser.fromJson(content, listType); 
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listaConnessioni;
    }

    static ArrayList<Direction> updateListDirFromJson (ArrayList<Direction> dir, Gson parser) {
        try {
            String content = Files.readString(Paths.get("./res/exec.json"));
            Type listType = new TypeToken<ArrayList<Server>>(){}.getType();
            dir = parser.fromJson(content, listType); 
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dir;
    }

    static void deletConnectionsRecord(ArrayList<Server> listaconnessioni) {
        Console console = System.console();
        String response = console.readLine("Si desidera eliminare il record delle connessioni? [y/n]");
        
        while(true){
            if(response.equals("y")){
                
                listaconnessioni.clear();
                
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

    static int getHigherID (ResultSet rs) {
        int higher = 1;
        try{
            while(rs.next()){
            if(rs.getInt("OBJECTID") >= higher){
                higher = rs.getInt("OBJECTID");
            }
            }
        } catch (Exception e){
            System.out.println("errore nel conteggio dell'id piu grande" + higher);
        }
        return higher;
    }

     static void update (Server intea, Server acoset) {
         try {
            Serverconnection inte = new Serverconnection(intea); 
            Serverconnection acos = new Serverconnection(acoset);
           
            ResultSet res1 = inte.query("SELECT * FROM CONTATORI");
            int objid = 0;

            res1 = acos.query("SELECT * FROM EAM2GIS_OGGETTI");
            PreparedStatement ps = inte.getConnection().prepareStatement ("DELETE FROM CONTATORI");
            ps.executeUpdate();

            ps = inte.getConnection().prepareStatement("INSERT INTO CONTATORI (OBJECTID, COMUNE, VIA_DENOMINAZIONE, DATA_INS, DATA_AGG, D_STATO, POINT_X, POINT_Y) VALUES (?,?,?,?,?,?,?,?)");
            
            while(res1.next()){
                ps.setInt(1, objid);
                ps.setString(2, res1.getString("E2G_COMUNE"));
                ps.setString(3, res1.getString("E2G_INDIRIZZO"));
                ps.setString(4, res1.getString("E2G_DATA_CREAZIONE"));
                ps.setString(5, res1.getString("E2G_DATA_MODIFICA"));
                ps.setString(6, res1.getString("E2G_STATUS"));
                ps.setString(7, res1.getString("E2G_COOX"));
                ps.setString(8, res1.getString("E2G_COOY"));
                ps.executeUpdate(); 
                objid++;
            }

            CloneAgent cloneSERB = new CloneAgent(inte, acos, "acq_serbatoio");
            cloneSERB.updateTable();

            CloneAgent cloneCOND = new CloneAgent(inte, acos, "acq_condotta");
            cloneCOND.updateTable();

            CloneAgent clonePOZZ = new CloneAgent(inte, acos, "acq_pozzo");
            clonePOZZ.updateTable();

            CloneAgent cloneCAM = new CloneAgent(inte, acos, "acq_cameretta");
            cloneCAM.updateTable();

            inte.close();
            acos.close();

        } catch (Exception e) {
            System.err.println(e);
        }
    }   
    
}

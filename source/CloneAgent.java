package source;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.time.*;
import java.sql.Timestamp;
import java.util.logging.*;;

public class CloneAgent {

    ArrayList <String> campi = new ArrayList<>();
    ArrayList <String> type = new ArrayList<>(); 

    String nomeTabella;
    Serverconnection src;
    Serverconnection trg;

    Logger logger = GlobalLogger.getLogger();

    public CloneAgent (Serverconnection src, Serverconnection trg, String nomeTabella) throws Exception{
        
        this.src =src;
        this.trg =trg;
        this.nomeTabella = nomeTabella;

        ResultSet nomecolonne = src.query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ='" + nomeTabella + "' ;");
        while(nomecolonne.next()){
            campi.add(nomecolonne.getString("COLUMN_NAME"));
        }

        ResultSet tipocolonne = src.query("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ='" + nomeTabella + "' ;");
        while(tipocolonne.next()){
            type.add(tipocolonne.getString("DATA_TYPE"));
        }
    }

    public void cloneTable () throws Exception{

        ResultSet copy = src.query("SELECT * FROM " + nomeTabella);

        String DeleteStatement = "DELETE FROM " + nomeTabella;
        PreparedStatement ps = trg.getConnection().prepareStatement(DeleteStatement);
        ps.executeUpdate();

        String statement = "INSERT INTO " + nomeTabella +" (";
        
        for(String campo : campi){
            statement = statement + campo + ", ";
        }
         
        statement = statement.substring(0, statement.length()-2);
        statement = statement + ") VALUES (";
       
        int j = 0;
        for(String campo : campi){
            statement = statement + "?, ";

        }

        statement = statement.substring(0, statement.length()-2);
        statement = statement + ")";
        //System.out.println(statement);

        ps = trg.getConnection().prepareStatement(statement);

        int numberDone = 0;
        while(copy.next()){
            int i = 0;
            int ncampo = 1;
            for(String campo : campi){
                if(type.get(i).equals("int") || type.get(i).equals("smallint") || type.get(i).equals("numeric")){
                    ps.setInt(ncampo, copy.getInt(campo));
                } else if (type.get(i).equals("geometry") || type.get(i).equals("varbinary")){
                    ps.setBytes(ncampo, copy.getBytes(campo));
                } else {
                    ps.setString(ncampo, copy.getString(campo));
                } 
                i++;
                ncampo++;
            }
            ps.executeUpdate();
            numberDone++;
            System.out.print("\r" + numberDone + " elementi copiati in " + nomeTabella);
            
        }
        logger.info(numberDone + " elementi copiati in " + nomeTabella);
        System.out.println();
    }

    public void updateTable () throws Exception{
        Timestamp updateTime = Timestamp.valueOf(LocalDate.now().minusDays(1).atStartOfDay());

    
        PreparedStatement deletedItems = trg.getConnection().prepareStatement("SELECT * FROM " + nomeTabella);
        ResultSet ceck = deletedItems.executeQuery();

        int count = 0;
        
        while(ceck.next()){
            int ex = 0;
           
            PreparedStatement exist = src.getConnection().prepareStatement("SELECT * FROM  " + nomeTabella + " WHERE OBJECTID = ?");
            exist.setString(1, ceck.getString("OBJECTID"));
            ResultSet existRes = exist.executeQuery();
            
            while(existRes.next()){
                ex++;
            }
           
            if(ex == 0){
                PreparedStatement delete = trg.getConnection().prepareStatement("DELETE FROM " + nomeTabella + " WHERE OBJECTID = ?");
                delete.setString(1, ceck.getString("OBJECTID"));
                delete.executeUpdate();
                System.out.println("\relemento cancellato cod id " + ceck.getString("OBJECTID"));
                logger.info("elemento cancellato cod id " + ceck.getString("OBJECTID"));
            }
            count++;
            System.out.print("\rcontrollo eliminazioni: "+ count);
            
        }
        logger.info("controllo eliminazioni: "+ count);
        System.out.println();

        PreparedStatement updated = src.getConnection().prepareStatement("SELECT * FROM  " + nomeTabella + " WHERE DATA_AGG >= ?");
        updated.setTimestamp(1, updateTime);
        ResultSet copy = updated.executeQuery();

        int v = 0;
        while(copy.next()){
            System.out.print("\r" + copy.getInt("OBJECTID"));
            try{
                PreparedStatement delete = trg.getConnection().prepareStatement("DELETE FROM  " + nomeTabella + " WHERE OBJECTID = ?");
                delete.setInt(1, copy.getInt("OBJECTID"));
                delete.executeUpdate();
           } catch (Exception e) {
                System.out.println(e);
                logger.severe(e.toString());
           }
           v++;
        }

        if(v == 0){
            System.out.print("Everything up to date on " + nomeTabella);
            logger.info("Everything up to date on " + nomeTabella);
        }

        String statement = "INSERT INTO " + nomeTabella +" (";
        
        for(String campo : campi){
            statement = statement + campo + ", ";
        }
         
        statement = statement.substring(0, statement.length()-2);
        statement = statement + ") VALUES (";
       
        int j = 0;
        for(String campo : campi){
            statement = statement + "?, ";

        }

        statement = statement.substring(0, statement.length()-2);
        statement = statement + ")";

        try {
            updated = src.getConnection().prepareStatement("SELECT * FROM  " + nomeTabella + " WHERE DATA_AGG >= ?");
        updated.setTimestamp(1, updateTime);
        ResultSet update = updated.executeQuery();
        PreparedStatement ps1 = trg.getConnection().prepareStatement(statement);

        int numberDone = 0;
        while(update.next()){
            System.out.print("\r" + numberDone + " elementi copiati in " + nomeTabella);
            int i = 0;
            int ncampo = 1;
            for(String campo : campi){
                if(type.get(i).equals("int") || type.get(i).equals("smallint") || type.get(i).equals("numeric")){
                    ps1.setInt(ncampo, update.getInt(campo));
                } else if (type.get(i).equals("geometry") || type.get(i).equals("varbinary")){
                    ps1.setBytes(ncampo, update.getBytes(campo));
                } else {
                    ps1.setString(ncampo, update.getString(campo));
                } 
                i++;
                ncampo++;
            }
            ps1.executeUpdate();
            numberDone++;
            System.out.print("\r" + numberDone + " elementi copiati in " + nomeTabella);
            logger.info(numberDone + " elementi copiati in " + nomeTabella);
        }
        
        } catch (Exception e) {
           
        }

          System.out.println();
    }

    public void updateContatori () throws Exception{
        Timestamp updateTime = Timestamp.valueOf(LocalDate.now().atStartOfDay());

        PreparedStatement updated = src.getConnection().prepareStatement("SELECT * FROM  " + nomeTabella + " WHERE E2G_DATA_MODIFICA >= ?");
        updated.setTimestamp(1, updateTime);
        ResultSet copy = updated.executeQuery();

        PreparedStatement deletedItems = trg.getConnection().prepareStatement("SELECT * FROM  CONTATORI");
        ResultSet ceck = deletedItems.executeQuery();

        int count = 0;
        int cancellati = 0;
        while(ceck.next()){
            int ex = 0;
            PreparedStatement exist = src.getConnection().prepareStatement("SELECT * FROM  " + nomeTabella + " WHERE E2G_CODE = ?");
            exist.setString(1, ceck.getString("PUF_CODE"));
            ResultSet existRes = exist.executeQuery();
            
            while(existRes.next()){
                ex++;
            }
           
            if(ex == 0){
                PreparedStatement delete = trg.getConnection().prepareStatement("DELETE FROM CONTATORI WHERE PUF_CODE = ?");
                delete.setString(1, ceck.getString("PUF_CODE"));
                delete.executeUpdate();
                cancellati ++;
            }
            count++;
            System.out.print("\rcontrollo eliminazioni: " + count);
        }
        logger.info("controllo eliminazioni: " + count);
        System.out.println();

        int v = 0;
        while(copy.next()){
            System.out.println("aggiornato: " + copy.getString("E2G_CODE"));
            try{
                PreparedStatement delete = trg.getConnection().prepareStatement("DELETE FROM CONTATORI WHERE PUF_CODE = ?");
                delete.setString(1, copy.getString("E2G_CODE"));
                delete.executeUpdate();
           } catch (Exception e) {
                System.out.println(e);
           }
           v++;
        }

        if(v == 0){
            System.out.println("Everything up to date on " + nomeTabella);
            logger.info("Everything up to date on " + nomeTabella);
        }

        
        PreparedStatement ps = trg.getConnection().prepareStatement("INSERT INTO CONTATORI (OBJECTID, COMUNE, VIA_DENOMINAZIONE, DATA_INS, DATA_AGG, D_STATO, POINT_X, POINT_Y, PUF_CODE) VALUES (?,?,?,?,?,?,?,?,?)");
        
        PreparedStatement idlist = trg.getConnection().prepareStatement("SELECT * FROM  CONTATORI");
        copy = idlist.executeQuery();
        int objid = getHigherID(copy) + 1;

        copy = updated.executeQuery();
        while(copy.next()){
            ps.setInt(1, objid);
            ps.setString(2, copy.getString("E2G_COMUNE"));
            ps.setString(3, copy.getString("E2G_INDIRIZZO"));
            ps.setString(4, copy.getString("E2G_DATA_CREAZIONE"));
            ps.setString(5, copy.getString("E2G_DATA_MODIFICA"));
            ps.setString(6, copy.getString("E2G_STATUS"));
            ps.setString(7, copy.getString("E2G_COOX"));
            ps.setString(8, copy.getString("E2G_COOY"));
            ps.setString(9, copy.getString("E2G_CODE"));
            ps.executeUpdate(); 
            objid++;
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
            System.out.println(e);
        }
        return higher;
    }
}
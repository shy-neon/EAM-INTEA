package source;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.time.*;
import java.sql.Timestamp;

public class CloneAgent {

    ArrayList <String> campi = new ArrayList<>();
    ArrayList <String> type = new ArrayList<>(); 

    String nomeTabella;
    Serverconnection src;
    Serverconnection trg;

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
             
    }

    public void updateTable () throws Exception{
        Timestamp updateTime = Timestamp.valueOf(LocalDate.now().minusDays(1).atStartOfDay());

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
           }
           v++;
        }

        if(v == 0){
            System.out.println("Everything up to date on " + nomeTabella);
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
        }
        } catch (Exception e) {
           System.out.println(e);
        }
    }
}
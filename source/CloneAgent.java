package source;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

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


        String statement = "INSERT INTO " + nomeTabella +" (";
        for(String campo : campi){
            statement = statement + campo + ", ";
        }
        statement = statement.substring(0, statement.length()-2);
        statement = statement + ") VALUES (";
        for(String campo : campi){
            statement = statement + "?, ";
        }
        statement = statement.substring(0, statement.length()-2);
        statement = statement + ")";
        System.out.println(statement);

        PreparedStatement ps = trg.getConnection().prepareStatement(statement);

        while(copy.next()){
            int i = 0;
            int ncampo = 1;
            for(String campo : campi){
                
                if(type.get(i).equals("int") || type.get(i).equals("smallint") || type.get(i).equals("varbinary") || type.get(i).equals("numeric")){
                    ps.setInt(ncampo, copy.getInt(campo));
                } else {
                    ps.setString(ncampo, copy.getString(campo));
                }
                i++;
                ncampo++;
            }
            ps.executeUpdate();
        }
             
    }
}

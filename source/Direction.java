package source;

import java.util.ArrayList;

public class Direction {
    private int type;
    private Server SRCServer;
    private Server TRGServer;
    public String nometabella;

    public Direction (String tipo, String src, String trg, ArrayList<Server> connectionlist, String nometabella) {
        if(tipo.equals("1")){
            this.type = 1;
        } else {
            this.type = 0;
        }

        this.nometabella = nometabella;

        for(Server server : connectionlist){
            if((server.nome).equals(src)){
                this.SRCServer = server;
            } 
        }

        for(Server server : connectionlist){
            if((server.nome).equals(trg)){
                this.TRGServer = server;
            }
        }
    }

    public Server getSourceServer() {
        return SRCServer;
    }

    public int getType() {
        return this.type;
    }

    public Server getTargetServer() {
        return TRGServer;
    }

    public void printDirection () {
        System.out.print("tipo:");
        if(this.type == 0){
            System.out.println(" COPIA");
        } else {
            System.out.println(" UPDATE");
        }
        System.out.println("Server source: " + SRCServer.nome);
        System.out.println("Server target: " + TRGServer.nome);
        System.out.println("Tabella: " + this.nometabella + "\n");
    }
}

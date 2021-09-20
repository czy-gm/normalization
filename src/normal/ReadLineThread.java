package normal;

import java.nio.MappedByteBuffer;

class ReadLineThread extends Thread {
    int line;
    int start;
    int end;
    MappedByteBuffer buff;
    public ReadLineThread(int start, int end, MappedByteBuffer buff) {
        this.buff=buff;
        this.end=end;
        this.start=start;
    }

    @Override
    public void run() {
        int pos=this.start;
        while (pos<this.end){
            if(buff.get(pos)=='\n'){
                this.line++;
            }
            pos++;
        }
        if (buff.get(this.end-1) != '\n') {
            this.line++;
        }
    }
}
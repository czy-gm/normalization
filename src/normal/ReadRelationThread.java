package normal;

import java.nio.MappedByteBuffer;

class ReadRelationThread extends Thread {
    int start_raw;
    int start;
    int end;
    MappedByteBuffer buff;

    public ReadRelationThread(int start_raw, int start, int end, MappedByteBuffer buff) {
        this.start_raw = start_raw;
        this.start = start;
        this.end = end;
        this.buff = buff;
    }

    @Override
    public void run() {
        /*int pos = this.start;
        int value = 0;
        while (pos < this.end) {

            while (buff.get(pos) != ' ') {
                value = buff.get(pos) - '0' + value * 10;
                pos++;
            }
            Main.edges[start_raw][0] = value;
            value = 0;
            pos++;
            while (buff.get(pos) != ' ') {
                pos++;
            }
            pos++;
            while (buff.get(pos) != ' ') {
                value = buff.get(pos) - '0' + value * 10;
                pos++;
            }
            Main.edges[start_raw][1] = value;
            value = 0;
            pos++;
            while (buff.get(pos) != '\n') {
                pos++;
            }
            pos++;
            start_raw++;
        }

         */
    }
}
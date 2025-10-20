/*
 *
 * Name: Benjamin Davison
 * Student ID: c4009899
 *
 */
import java.io.*;
import java.net.*;
import java.sql.SQLOutput;
//  (AtomicInteger imported to make the alternating sequence number a less complicated soluton to use throught different subroutines
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class Protocol {

    static final String NORMAL_MODE = "nm";         // normal transfer mode: (for Part 1 and 2)
    static final String TIMEOUT_MODE = "wt";        // timeout transfer mode: (for Part 3)
    static final String LOST_MODE = "wl";           // lost Ack transfer mode: (for Part 4)
    static final int DEFAULT_TIMEOUT = 1000;         // default timeout in milliseconds (for Part 3)
    static final int DEFAULT_RETRIES = 4;            // default number of consecutive retries (for Part 3)
    public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

    /*
     * The following attributes control the execution of the transfer protocol and provide access to the
     * resources needed for the transfer
     *
     */

    private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
    private int portNumber;            // the  port the server is listening on
    private DatagramSocket socket;      // the socket that the client binds to

    private File inputFile;            // the client-side CSV file that has the readings to transfer
    private String outputFileName;    // the name of the output file to create on the server to store the readings
    private int maxPatchSize;           // the patch size - no of readings to be sent in the payload of a single Data segment

    private Segment dataSeg;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server
    private Segment ackSeg;          // the protocol Ack segment for receiving ACK segments from the server

    private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
    private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
    private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

    private int fileTotalReadings;    // number of all readings in the csv file
    private int sentReadings;         // number of readings successfully sent and acknowledged
    private int totalSegments;        // total segments that the client sent to the server


    // Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
    public static Protocol instance = new Protocol();

    /**************************************************************************************************************************************
     **************************************************************************************************************************************
     * For this assignment, you have to implement the following methods:
     *		sendMetadata()
     *      readandSend()
     *      receiveAck()
     *      startTimeoutWithRetransmission()
     *		receiveWithAckLoss()
     * Do not change any method signatures, and do not change any other methods or code provided.
     ***************************************************************************************************************************************
     **************************************************************************************************************************************/
    /*
     * This method sends protocol metadata to the server.
     * See coursework specification for full details.
     */
    public void sendMetadata() throws IOException {
        BufferedReader csvReader;
        int totalNumOfReadings = 0;

        try {
            csvReader = new BufferedReader(new FileReader(inputFile));

            while ((csvReader.readLine()) != null) {
                totalNumOfReadings++;

            }

            csvReader.close();


        } catch (FileNotFoundException e) {
            System.out.println("The CSV file contains no lines of data/file not located" + e.getMessage());

            //Catch clause for the line variable assignment to the number of lines read by the buffer reader
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        fileTotalReadings = totalNumOfReadings;

        // Creating a payload for the segment object
        String segmentPayload = fileTotalReadings + "," + outputFileName + "," + maxPatchSize;

        // Creating a segment object that can later be sent to the server
        Segment dataSeg = new Segment(0, SegmentType.Meta, segmentPayload, segmentPayload.length());


        try {
            //  Sending the segment to the server
            ByteArrayOutputStream segmentOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream segmentObjectStream = new ObjectOutputStream(segmentOutputStream);

            //  Write the segment to the byte object - to e sent to the server
            segmentObjectStream.writeObject(dataSeg);
            segmentObjectStream.flush();

            //  Create byte stream for the segment to be transferred to server
            byte[] segmentByteStream = segmentOutputStream.toByteArray();

            //  Create data packet and input the segment byte stream as the 'buffer' parameter for the data packet
            DatagramPacket segmentPacket = new DatagramPacket(segmentByteStream, segmentByteStream.length, ipAddress, portNumber);
            socket.send(segmentPacket);
            System.out.println("CLIENT: META [SEQ#" + dataSeg.getSeqNum() + "] (Number of readings:" + fileTotalReadings + ", file name:" + outputFileName + ", patch size:" + maxPatchSize + ")");

            segmentOutputStream.close();
            segmentObjectStream.close();

        } catch (IOException e) {
            System.out.println("CLIENT: Failed to send segment data to client " + e.getMessage());
            System.exit(1);
        }


    }


    /*
     * This method read and send the next data segment (dataSeg) to the server.
     * See coursework specification for full details.
     */
    public void readAndSend() throws IOException {
        //  Create a new buffer to send data segments for 'readAndSend()'
        BufferedReader csvReader = new BufferedReader(new FileReader(inputFile));

        for (int i = 0; i < sentReadings; i++) {
            csvReader.readLine();
        }

        StringBuilder payloadCreation = new StringBuilder();
        int countingPatchReadings = 0;
        String line;

        //  Adding semicolons between each reading - or if there is only one reading display the reading for that line
        while ((countingPatchReadings < maxPatchSize) && ((line = csvReader.readLine()) != null)) {
            if (countingPatchReadings > 0) {
                payloadCreation.append(";");
            }

            //	#FORMATING CSV FILE
            //	Separating the csv file into its individual values (splitting the line after each 'readLine' is executed)
            String[] individualCSV = line.split(",");

            //	Initialising the float array for the set of 3 float values:
            float[] floatValues = new float[3];
            //	Setting the values at indexes; 2, 3 and 4, to be the first three index positions of the new array
            floatValues[0] = Float.parseFloat(individualCSV[2].trim());
            floatValues[1] = Float.parseFloat(individualCSV[3].trim());
            floatValues[2] = Float.parseFloat(individualCSV[4].trim());

            //	Initialising the columns with their own variables
            String sensorID = individualCSV[0];
            long timeStamp = Long.parseLong(individualCSV[1]);    //	timeStamp won't work as an int - value to small - has to be cast the string of arrays to a 'long' datatype tomaccomodate the timestamp.

            //	Creating a 'Reading' object with the new values that have been created:
            Reading formatReading = new Reading(sensorID, timeStamp, floatValues);
            //	Append the reading toString to the segment payload string:
            payloadCreation.append(formatReading.toString());
            countingPatchReadings++;
        }

        csvReader.close();

        //  Validation check for empty file (after all readings have been accounted for)
        if (countingPatchReadings == 0) {
            //	All readings have been collated into segments - the total segments should be displayed at the end of the Client terminal.
            System.out.println("Total segments: " + totalSegments);
            System.exit(0);
        }

        //  Initialise variables for the payload data
        String activePayload = payloadCreation.toString();
        int lengthOfPayload = activePayload.length();

        //	Adding 1 to the total segments (which will initially be 0) will increase the value of total segments to 1,
        //	MOD 2 leaves the remainder as 1 for the first sequence number, this will display the SEQ# format to start at
        //	1 for the actual readings, in compliance with the model output in the specification.
        int payloadSeqNum = ((totalSegments + 1) % 2);
        dataSeg = new Segment(payloadSeqNum, SegmentType.Data, activePayload, lengthOfPayload);

        //  Sending the segment to the server
        ByteArrayOutputStream dataSegOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream dataSegObjectStream = new ObjectOutputStream(dataSegOutputStream);

        //  Write the segment to the byte object - to e sent to the server
        dataSegObjectStream.writeObject(dataSeg);
        //  Send the contents of the byte array output stream to the object
        dataSegObjectStream.flush();

        //  Byte Stream Conversion
        byte[] dataSegByteStream = dataSegOutputStream.toByteArray();

        //  Packets created for next segments to be sent
        DatagramPacket dataSegPacket = new DatagramPacket(dataSegByteStream, dataSegByteStream.length, ipAddress, portNumber);
        socket.send(dataSegPacket);

        //  Assign new values to variables after the packets have been sent as these will be now acknowledged by the server
        sentReadings += countingPatchReadings;
        //  Increment the total segments counter after the segment has been sent to the server in a packet
        totalSegments++;

        //  Displaying the output statistics of the segment packet
        System.out.println("CLIENT: Send: DATA [SEQ#" + dataSeg.getSeqNum() + "] (size:" + lengthOfPayload + "crc:" + dataSeg.calculateChecksum() + "content:" + activePayload + ")");
        dataSegOutputStream.close();
        dataSegObjectStream.close();

    }

    /*
     * This method receives the current Ack segment (ackSeg) from the server
     * See coursework specification for full details.
     */
    public boolean receiveAck() throws IOException {
        //  Create a new buffer for the ACK data packet
        byte[] ackBuffer = new byte[MAX_Segment_SIZE];  //  'MAX_SEGMENT_SIZE' is used to account for every segment sent - so the buffer won't exceed memory during runtime
        //  Packet being created for the ACK segment to be received by the client from the server.
        DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);

        //  Try statement created to check if the client will receive the packet from the server
        //	The packet is established and requests to be received on the client-side via the server - this will confirm the connection between server and client.
        socket.receive(ackPacket);

        //  Converting the ACK packet into a byte stream so it can be sent through within the segment to be received by the server.
        ByteArrayInputStream ackInputStream = new ByteArrayInputStream(ackBuffer);
        ObjectInputStream ackObjectStream = new ObjectInputStream(ackInputStream);

        //  Creating the ACK segment (using code almost identical to the 'Server' java file).
        try {
            //  Testing if the raw data of the (object) buffer can be compiled and created into a segment data type.
            ackSeg = (Segment) ackObjectStream.readObject();    //	Creates segment object using the data transferred from the segment object set up in 'Server'.
        } catch (ClassNotFoundException e) {
            System.out.println("CLIENT: Failed to read ack segment from client " + e.getMessage());
        }

        //  Checking if the type of the segment that has just been created is of the same 'Ack' type specified in the segment 'enum' datatype.
        if (ackSeg.getType() == SegmentType.Ack) {
            if (ackSeg.getSeqNum() == dataSeg.getSeqNum()) {
                System.out.println("CLIENT: RECEIVE: ACK[SEQ#" + ackSeg.getSeqNum() + "]");
                System.out.println("------------------------------------------------------------------");
                ackInputStream.close();
                ackObjectStream.close();
                return true;
            }

        } else {
            //  (if the types of the segments don't match)
            System.out.println("CLIENT: Failed to create ACK segment of type 'Ack'");
            return false;
        }

        return false;
    }

    /*
     * This method starts a timer and does re-transmission of the Data segment
     * See coursework specification for full details.
     */
    public void startTimeoutWithRetransmission() throws IOException {
        try {
            socket.setSoTimeout(timeout);
        } catch (SocketException e) {
            System.out.println("CLIENT: Unable to apply a socket timeout: " + e.getMessage());
            System.exit(1);
        }
        //	After the last Ack loss, this loop will continue loop around until either a valid Ack seg has been located, or the max number of retries has been hit.
        //	This loop will almost always be true (depending on the probability)
        while (currRetry < maxRetries) {
            try {
                if (receiveAck()) {    //	If 'receiveAck' is true, then the client received the same segment as the server / a valid Ack segment has been produced.
                    currRetry = 0;    //	Lost Ack retry counter is reset after a valid ack segment has passed.
                    break;
                }

            } catch (Exception e) {
                //	If no Ack segment is received, and the 'receive()' method for the socket times out -
                //	the Lost Ack counter is incremented by one, as well as the totalSegments value (as per the spec - each time a segment is transferred this value must be adjusted).
                currRetry++;
                totalSegments++;

                //	Re-transmitting the lost segment
                System.out.println("CLIENT: Socket Timeout - ACK[SEQ#" + dataSeg.getSeqNum() + "has been lost");

                //	Segment data already read from the input streams in 'receiveAck' - so output streams must be used to write the segment data to the object stream to transfer over a network.
                ByteArrayOutputStream lostSegOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream lostSegObjectStream = new ObjectOutputStream(lostSegOutputStream);

                //	Write the contents of the dataSeg to the object stream
                lostSegObjectStream.writeObject(dataSeg);
                lostSegObjectStream.flush();

                //	Create a byte stream as a buffer for the lost Ack segments packet
                byte[] lostSegByteStream = lostSegOutputStream.toByteArray();

                //	Lost segment packet creation:
                DatagramPacket lostSegPacket = new DatagramPacket(lostSegByteStream, lostSegByteStream.length, ipAddress, portNumber);

                //	Re-send the packet back to the server
                socket.send(lostSegPacket);

                lostSegOutputStream.close();
                lostSegObjectStream.close();
            }
        }

        //	If the number of retires for the client is less than the number of retries of a lost Ack segment,
        //	the client must terminate as the maximum number of retries to resend a singular segment has been exceeded.
        if (maxRetries <= currRetry) {
            System.out.println("CLIENT: Maximum number of retries permitted for this retransmission - Socket --> Closed");
            System.out.println("CLIENT: Closed");
            //	Abnormal termination - program can't continue if the max value is exceeded
            System.exit(1);
        }
    }


    /*
     * This method is used by the server to receive the Data segment in Lost Ack mode
     * See coursework specification for full details.
     */
    public void receiveWithAckLoss(DatagramSocket serverSocket, float loss) throws IOException {

        try {
            serverSocket.setSoTimeout(2000);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        //  Variable Initialisation
        byte[] buf = new byte[Protocol.MAX_Segment_SIZE];

        //  Creating a list to store duplicate segment readings to remove from the final list made by the server
        List<String> tempReadings = new ArrayList<>();

        int seqNumOutcome = 1;
        int lastKnownAckSeqNum = 0;
        int byteTotal = 0;
        int usefulByteTotal = 0;

        //  CODE FROM 'SERVER' - START
        int readingCount= 0;

        // while still receiving Data segments
        while (true) {
            DatagramPacket incomingPacket = new DatagramPacket(buf, buf.length);
            serverSocket.receive(incomingPacket);// receive from the client

            Segment serverDataSeg = new Segment();
            byte[] data = incomingPacket.getData();
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);

            // read and then print the content of the segment
            try {
                serverDataSeg = (Segment) is.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            System.out.println("SERVER: Receive: DATA [SEQ#" + serverDataSeg.getSeqNum() + "](" + "size:" + serverDataSeg.getSize() + ", crc: " + serverDataSeg.getChecksum() +
                    ", content:" + serverDataSeg.getPayLoad() + ")");

            // calculate the checksum
            long x = serverDataSeg.calculateChecksum();

            // if the calculated checksum is same as that of received checksum then send the corresponding ack
            if (serverDataSeg.getType() == SegmentType.Data && x == serverDataSeg.getChecksum()) {
                if (serverDataSeg.getSeqNum() == dataSeg.getSeqNum()) {
                    System.out.println("SERVER: Calculated checksum is " + x + "  VALID");
                    tempReadings.add(serverDataSeg.getPayLoad());

                    //update the number of correctly received readings
                    readingCount++;

                    //  Ack loss simulation
                    if (isLost((loss))) {
                        System.out.println("SERVER: Ack loss simulation - ACK{SEQ#" + seqNumOutcome + "} has been lost");
                    } else {
                        //  If the Ack segment isn't lost in the simulation, the server will resend the Ack
                        Server.sendAck(serverSocket, incomingPacket.getAddress(), incomingPacket.getPort(), serverDataSeg.getSeqNum());
                    }

                    //  Alternates the sequence number for future lost Acks (changes the seq num from 1-->0, 0-->1)
                    seqNumOutcome = seqNumOutcome - 1;
                } else {
                    //  Duplicate data segment:
                    System.out.println("SERVER: Duplicate data located - resending previous ACK");

                    if (isLost((loss))) {
                        System.out.println("SERVER: Ack loss simulation - ACK{SEQ#" + (1 - seqNumOutcome) + "} has been lost");
                    } else {
                        Server.sendAck(serverSocket, incomingPacket.getAddress(), incomingPacket.getPort(), (1 - seqNumOutcome));
                    }
                }
            }

            double ackEfficiency = ((double) usefulByteTotal / totalSegments) * 100;
            System.out.println("Efficiency: " + ackEfficiency + "%");



            }
    }



	/*************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	These methods are implemented for you .. Do NOT Change them 
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************/	 
	/* 
	 * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
	 */
	public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
		instance.ipAddress = InetAddress.getByName(hostName);
		instance.portNumber = Integer.parseInt(portNumber);
		instance.socket = new DatagramSocket();

		instance.inputFile = checkFile(fileName); //check if the CSV file does exist
		instance.outputFileName =  outputFileName;
		instance.maxPatchSize= Integer.parseInt(batchSize);

		instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
		instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

		instance.fileTotalReadings = 0; 
		instance.sentReadings=0;
		instance.totalSegments =0;

		instance.timeout = DEFAULT_TIMEOUT;
		instance.maxRetries = DEFAULT_RETRIES;
		instance.currRetry = 0;		 
	}


	/* 
	 * check if the csv file does exist before sending it 
	 */
	private static File checkFile(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists()) {
			System.out.println("CLIENT: File does not exists"); 
			System.out.println("CLIENT: Exit .."); 
			System.exit(0);
		}
		return file;
	}

	/* 
	 * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
	 */
	private static Boolean isLost(float prob) 
	{ 
		double randomValue = Math.random();  //0.0 to 99.9
		return randomValue <= prob;
	}

	/* 
	 * getter and setter methods	 *
	 */
	public String getOutputFileName() {
		return outputFileName;
	} 

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	} 

	public int getMaxPatchSize() {
		return maxPatchSize;
	} 

	public void setMaxPatchSize(int maxPatchSize) {
		this.maxPatchSize = maxPatchSize;
	} 

	public int getFileTotalReadings() {
		return fileTotalReadings;
	} 

	public void setFileTotalReadings(int fileTotalReadings) {
		this.fileTotalReadings = fileTotalReadings;
	}

	public void setDataSeg(Segment dataSeg) {
		this.dataSeg = dataSeg;
	}

	public void setAckSeg(Segment ackSeg) {
		this.ackSeg = ackSeg;
	}

	public void setCurrRetry(int currRetry) {
		this.currRetry = currRetry;
	}

}

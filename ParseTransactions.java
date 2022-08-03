import java.io.*;
import java.lang.*;
import java.nio.*;
import java.util.*;

/**
 * Parse Binary Transaction Log  using low level byte streams .
 * @
 * Sri Ganesh V Madhukar Reddy
 */
public class ParseTransactions {
	private static final int BUFFER_SIZE = 4096; // 4KB
	private static int DEBUG=2;
	public String inputFile;
	public String outputFile;
    public InputStream inputStream; 
    public OutputStream outputStream;
	
	static float 	total_credits=0;
	static float 	total_debits=0;
	static int	  	autopays_started=0;
	static int	  	autopays_ended=0;
	static float 	user_balance=0;
	static String userIdStr = "2456938384156277127";
	static long userIdLong=0;
	
	
	enum TransactionType {
		Debit, Credit, StartAutoPay, EndAutoPay
	}
	
    public ParseTransactions(InputStream inputStream, OutputStream outputStream)
	{
		this.inputStream = inputStream;
		this.outputStream = outputStream;
	}
	
	public int parse_records()
	{

		try
		{
            
			byte[] buffer4 = new byte[4];
			byte[] buffer8 = new byte[8];
			byte[] buffer4K = new byte[4096];
			
            int bytesRead = -1;
			String stringValue;
			String magicValue = "MPS7";
			int	version = -1;
			int noOfRecords = 0;
			int transType = 0;
			ByteBuffer bb;
			long	user_id;
			float	credit_amount;
			float	debit_amount;
			float	amount;
			int		auto_start;
			int		auto_end;
			
			userIdLong = Long.valueOf(userIdStr);

			
 
			if ((bytesRead = inputStream.read(buffer4)) < 0) {
				System.out.println("Input File Empty");
				return (-1);
			}
			
			stringValue = "" + buffer4;
			int i=0;
			bb = ByteBuffer.wrap(buffer4);
			String magicString = new String(bb.array(), "UTF-8");
			// if (buffer4[i++] == 'M' && buffer4[i++] == 'P' && buffer4[i++] == 'S' && buffer4[i++] == '7')
			if (magicString.equals(magicValue))
			{
				// System.out.println("buffer Magic Value Matched " + magicString);
			} else {
				System.out.println("Magic File did not match");		
				return (-1);
			}
			/*
			 * Read Version
			 */
			if ((version = inputStream.read()) < 0) {
				System.out.println("Error reading header; End of file reached");
				return (-1);
			}
			else {
				// System.out.println("version = " + version);
			}
			i = 0;
			/*
			 * Read No Of Records
			 */
			if ((bytesRead = inputStream.read(buffer4)) < 0) {
				System.out.println("Error reading header : No Of Records; End of file reached");
				return (-1);
			}
			else {
				bb = ByteBuffer.wrap(buffer4);
			//	System.out.println(buffer4[3] + " : Total: No of Records = " + buffer4[i++] + buffer4[i++] + buffer4[i++] + buffer4[i++]);
			//	System.out.println(bb.getInt() + " : Total No of Records");
			}
		if (DEBUG>0) {System.out.println("\n Record Type Unix timestamp   user ID            amount in dollars\n"); }
		
		while ((transType = inputStream.read()) != -1) {

			// System.out.println("\n" + TransactionType.values()[transType]);
			if (DEBUG>0) {System.out.print("\n" + TransactionType.values()[transType]);}
			/*
			 * Timestamp
			 */
			if ((bytesRead = inputStream.read(buffer4)) < 0) {
				System.out.println("Error reading Next record : End of file reached");
				return (-1);
			}
			else {
				bb = ByteBuffer.wrap(buffer4);
				// System.out.println("Timestamp : " + bb.getInt());
				if (DEBUG>0) {System.out.print(" " + bb.getInt()); }
			}
			/*
			 * USERID
			 */

			if ((bytesRead = inputStream.read(buffer8)) < 0) {
				System.out.println("Error reading Next record : End of file reached");
				return (-1);
			}
			else {
				bb = ByteBuffer.wrap(buffer8);
				// System.out.println("UserID : " + bb.getLong()); 
				user_id = bb.getLong();
				if (DEBUG>0) { System.out.print(" " + user_id);}
			}
			/*
			 * AMOUNTS
			 */			
			// if (transType == TransactionType.Debit || transType == TransactionType.Credit)

			if (transType == 0 || transType == 1)
			{
				if ((bytesRead = inputStream.read(buffer8)) < 0) {
					System.out.println("Error reading Next record : End of file reached");
					return (-1);
				}
				else {
					bb = ByteBuffer.wrap(buffer8);
					amount = bb.getFloat();
					// System.out.println("Amount : " + bb.getFloat()); 
					if (DEBUG>0) { System.out.print(" " + amount); }
				}
				if (transType == 0) {
					total_debits += amount;
					if (user_id == userIdLong) {
						user_balance += amount;
					}
				}
				else if (transType == 1) {
					total_credits += amount;
					if (user_id == userIdLong) {
						user_balance -= amount;
					}
				}
			}
			else if (transType == 2) {
				autopays_started++;
			}
			else if (transType == 3) {
				autopays_ended++;
			}
		}
		
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return(0);
	}
	
	
	public static void main(String[] args) 
	{
		
        if (args.length < 1) {
            System.out.println("Please provide input file");
            System.exit(0);
        }
 
        String inputFile = args[0];
        // String outputFile = args[1];
		String outputFile = "transactionsLog.dat";
		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			DEBUG = Integer.parseInt(System.getenv("USER_DEBUG"));
		} catch (Exception ex) {
			DEBUG = -1;
		}
		
		// System.out.println("DEBUG=" + DEBUG);

        try {
			inputStream = new FileInputStream(inputFile);
            outputStream = new FileOutputStream(outputFile);
			 
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead = -1; // read one byte at a time ; ((byteRead = inputStream.read()) != -1)
  
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		ParseTransactions parseTransactions = new ParseTransactions(inputStream, outputStream);

		parseTransactions.parse_records();
		System.out.print("\n total credit amount=" + total_credits + 
			"\n total debit amount=" + total_debits +
			"\n autopays started=" + autopays_started + "\n autopays ended=" + autopays_ended + 
			"\n balance for user " + userIdLong + "=" + user_balance +
			"\n");
	}  
}



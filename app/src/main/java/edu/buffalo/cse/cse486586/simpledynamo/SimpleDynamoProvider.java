import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/*This class is useful for key,value storages,insertion of the key,values,deletion of the key,
 Searching of the key*/
public class SimpleDynamoProvider extends ContentProvider {
	private SQLiteDatabase grpMsgDb;
	private DataStorage dbStorage;
	public static final int SERVER_PORT = 10000;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	String myPort;
	private Uri mUri;
	ArrayList<String> portHashSortList;
	HashMap<String, String> portSuccMap;
	public static final String DB_KEY = "key";
	public static final String DB_VAL = "value";
	public static final String DB_TIME = "time";
	HashMap<String, String> portHashMap;
	boolean mutex = true;
	String queryData = null;
	String[] portsArr;
	HashMap<String, String> portPredMap;
	String SYNC_ALL = "syncAll";
	String myPortVals = "myPortVals";
	HashMap<String, String> queryMap;
	HashMap<String, Integer> insertAckMap;
	String INSERT_KEY_ACK = "insertKeyAck";

	/* This is called to delete the key from the DB */
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String key = selection;
		String msg = key + "delete";
		for (int i = 0; i < 5; i++) {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portsArr[i]);
		}
		return 0;
	}

	private int deleteKeyDb(String selection) {
		SQLiteDatabase dbk = dbStorage.getWritableDatabase();
		return dbk.delete(TABLE_NAME, "key='" + selection + "'", null);
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	/* This is called to insert the key into the DB */
	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String key = (String) values.getAsString(DB_KEY);
		String value = (String) values.getAsString(DB_VAL);
		String msg = key + "---" + value + "---" + myPort;
		String port = returnExcatPort(key);
		insertAckMap.remove(key);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portSuccMap.get(port).split("#")[0]);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portSuccMap.get(port).split("#")[1]);
		insertAckMap.put(key, 3);
		while (insertAckMap.get(key) > 1) {

		}
		return null;
	}

	/*This method is called to store the key,values in DB*/
	private void writeIntoDB(Uri uri, ContentValues values) {

		String key = (String) values.getAsString(DB_KEY);
		grpMsgDb = dbStorage.getWritableDatabase();
		long row_id = grpMsgDb.insertWithOnConflict(TABLE_NAME, null, values, 5);

		if (row_id != -1) {
			uri.withAppendedPath(uri, Long.toString(row_id));
		}
	}

	/*This method is called when emulator starts*/
	@Override
	public boolean onCreate() {

		dbStorage = new DataStorage(getContext());
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity");
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		initFunction();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			syncMyPortPredSuc();

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		} 
		return true;
	}

	/*This method is called to Sync the keys from Predecessor and Successor*/
	private void syncMyPortPredSuc() {
		String predPort = portPredMap.get(myPort);
		String succPort = portSuccMap.get(myPort).split("#")[0];
		String msg = myPort + SYNC_ALL;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, predPort);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, succPort);
	}

	/*This is the class of the server task*/
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Socket socket = serverSocket.accept();
					BufferedReader brReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

					String receivedMesssage = brReader.readLine();

					if (receivedMesssage.contains("---")) {// For Insertion
						ContentValues cv = new ContentValues();
						cv.put(DB_KEY, receivedMesssage.split("---")[0]);
						cv.put(DB_VAL, receivedMesssage.split("---")[1]);
						writeIntoDB(mUri, cv);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, receivedMesssage.split("---")[0] + INSERT_KEY_ACK, receivedMesssage.split("---")[2]);
					} else if (receivedMesssage.contains(INSERT_KEY_ACK)) { // For acknowledgement after insert
						receivedMesssage = receivedMesssage.replace(INSERT_KEY_ACK, "");
						synchronized (this) {
							int count = insertAckMap.get(receivedMesssage) - 1;
							insertAckMap.put(receivedMesssage, count);
						}
					} else if (receivedMesssage.contains("__")) {// For Querying single key
						queryReturnSingleValue(receivedMesssage);
					} else if (receivedMesssage.contains("%")) {// Recmsg contains % whenever it result came from Single key
						String key = receivedMesssage.split("%")[0];
						queryMap.put(key, receivedMesssage.split("%")[1]);
					} else if (receivedMesssage.contains("$")) {
						queryAllMsgs(receivedMesssage);
					} else if (receivedMesssage.contains("~~") && !receivedMesssage.contains(myPortVals)) {
						queryData = receivedMesssage;
						mutex = false;
					} else if (receivedMesssage.contains("delete")) {// For Deletion  key
						receivedMesssage = receivedMesssage.replace("delete", "");
						deleteKeyDb(receivedMesssage);
					} else if (receivedMesssage.contains(SYNC_ALL)) {// For Syncing  ports after node has recovered from failue
						querySyncPorts(receivedMesssage.replace(SYNC_ALL, ""));
					} else if (receivedMesssage.contains(myPortVals)) {//Got values from Predecessor and Successor
						receivedMesssage = receivedMesssage.replace(myPortVals, "");
						insertInDBMissedkeys(receivedMesssage);
					}
					brReader.close();
					socket.close();
				}

			} catch (IOException e) {
				Log.d(TAG, e.getMessage());
			}

			return null;
		}
	}

	/*This is the class of the client task*/
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			try {

				String port = msgs[1];

				Socket socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), Integer.parseInt(port));

				String msgToSend = msgs[0];

				PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
				pw.write(msgToSend);
				pw.flush();
				pw.close();

				socket.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException" + e.getMessage());
			}
			return null;
		}
	}

	/*This is used to insert missed keys into DB*/
	private void insertInDBMissedkeys(String msg) {
		String[] arr = msg.split("~~");
		String ky, val;
		for (int i = 0; i < arr.length; i++) {
			String st = arr[i];
			ky = st.split(",")[0];
			val = st.split(",")[1];
			ContentValues cv = new ContentValues();
			cv.put(DB_KEY, ky);
			cv.put(DB_VAL, val);
			writeIntoDB(mUri, cv);
		}
	}

	/*This is used to query a value for a key */
	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		String recvmsg = selection;
		if (!recvmsg.contains("@") && !recvmsg.contains("*")) {
			String key = recvmsg;
			String retPort = returnExcatPort(recvmsg);

			String msg = key + "__" + myPort;
			String port = retPort;
			String succ1 = portSuccMap.get(port).split("#")[0];
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, succ1);

			while (!queryMap.containsKey(key)) {

			}
			String keyff = key;
			String val11 = queryMap.get(key);

			String arr[] = new String[] { DB_KEY, DB_VAL };
			String rowa[] = new String[] { keyff, val11 };
			MatrixCursor mc = new MatrixCursor(arr);
			mc.addRow(rowa);
			queryMap = new HashMap<>();
			return mc;

		} else if (recvmsg.contains("@")) {
			String str = "select * from " + TABLE_NAME;
			SQLiteDatabase db = dbStorage.getReadableDatabase();
			Cursor cursor = db.rawQuery(str, null);
			return cursor;
		} else if (recvmsg.contains("*")) {
			String matArr[] = new String[] { DB_KEY, DB_VAL };
			MatrixCursor matCursor = new MatrixCursor(matArr);
			LinkedHashSet<String> set = new LinkedHashSet<>();
			for (int i = 0; i < portsArr.length; i++) {
				queryData = null;
				mutex = true;

				if (myPort.equals(portsArr[i])) {
					String localAllKeys = queryStarLocalMsgs();
					if (!localAllKeys.equals("~~")) {
						String[] arr = localAllKeys.split("~~");
						for (int k = 0; k < arr.length; k++) {
							set.add(arr[k]);
						}
					}
					continue;
				}

				String msg = myPort + "$";
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, portsArr[i]);
				long startTime = System.currentTimeMillis() / 1000;
				long timeOut;
				while (mutex) {
					timeOut = System.currentTimeMillis() / 1000 - startTime;
					if (timeOut > 2.1) {
						break;
					}
				}

				if (queryData != null && !queryData.equals("~~")) {
					String[] arr = queryData.split("~~");
					for (int k = 0; k < arr.length; k++) {
						set.add(arr[k]);
					}
				}
			}
			mutex = true;
			queryData = null;
			Iterator<String> it = set.iterator();
			String ky;
			String val;
			while (it.hasNext()) {
				String st = it.next();
				ky = st.split(",")[0];
				val = st.split(",")[1];
				String[] stm = new String[] { ky, val };
				matCursor.addRow(stm);
			}

			return matCursor;
		}
		return null;
	}

	/*This method is called to get the keys from other nodes*/
	private void querySyncPorts(String recMsg) {
		String portTosd = recMsg;
		String str = "select * from " + TABLE_NAME;
		SQLiteDatabase db = dbStorage.getReadableDatabase();
		Cursor cursor = db.rawQuery(str, null);

		String keyValPairs = "";
		String key = "";
		String value = "";
		while (cursor.moveToNext()) {
			key = cursor.getString(0);
			value = cursor.getString(1);
			String excatPort = returnExcatPort(key);
			boolean flag = isCrtPort(excatPort, portTosd);
			if (flag) {
				keyValPairs += (cursor.getString(0) + "," + cursor.getString(1));
				keyValPairs += "~~";
			}
		}

		if (keyValPairs == "") {
			Log.d("querySyncPortsEMpty keyvalues pairs ", myPort);
		} else {
			keyValPairs = keyValPairs + myPortVals;
		}

		if (!keyValPairs.isEmpty() && keyValPairs.length() > 0) {
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValPairs, portTosd);
		}
	}

	private boolean isCrtPort(String coPort, String portToSend) {
		String succ1 = portSuccMap.get(coPort).split("#")[0];
		String succ2 = portSuccMap.get(coPort).split("#")[1];
		if (portToSend.equals(coPort) || portToSend.equals(succ1) || portToSend.equals(succ2)) {
			return true;
		} else
			return false;
	}

	private String returnExcatPort(String key) {
		try {
			String hashkey = genHash(key);
			String hashvalMyPort = genHash(Integer.parseInt(myPort) / 2 + "");
			if (hashkey.compareTo(portHashSortList.get(0)) <= 0 || hashkey.compareTo(portHashSortList.get(4)) > 0) {
				String port = portHashMap.get(portHashSortList.get(0));
				return portHashMap.get(portHashSortList.get(0));
			} else {
				for (int i = 1; i < 5; i++) {
					if (hashkey.compareTo(portHashSortList.get(i - 1)) > 0 && hashkey.compareTo(portHashSortList.get(i)) <= 0) {
						return portHashMap.get(portHashSortList.get(i));
					}

				}
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "NoSuchAlgorithmException Exception");
		}
		return null;
	}

	
	private void queryReturnSingleValue(String recvmsg) {
		String key = recvmsg.split("__")[0];
		String portToSend = recvmsg.split("__")[1];
		Cursor cs = getSingleValue(key);
		try {
			while (cs.getCount() == 0) {
				synchronized (this) {
					this.wait(100);
				}
				cs = getSingleValue(key);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		cs.moveToFirst();
		String keyf = cs.getString(0);
		String valuef = cs.getString(1);
		String keyval = keyf + "%" + valuef;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyval, portToSend);
	}

	/* This function returns the all the key,values from all the nodes */
	private void queryAllMsgs(String recMsg) {
		String portTosd = recMsg.replace("$", "");
		String str = "select * from " + TABLE_NAME;
		SQLiteDatabase db = dbStorage.getReadableDatabase();
		Cursor cursor = db.rawQuery(str, null);

		String keyValPairs = "";

		while (cursor.moveToNext()) {
			keyValPairs += (cursor.getString(0) + "," + cursor.getString(1));
			keyValPairs += "~~";
		}

		if (keyValPairs == "") {
			keyValPairs = "~~";
		}
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyValPairs, portTosd);
	}

	/* This function returns the all the key,values from single  node */
	private String queryStarLocalMsgs() {
		String str = "select * from " + TABLE_NAME;
		SQLiteDatabase db = dbStorage.getReadableDatabase();
		Cursor cursor = db.rawQuery(str, null);

		String keyValPairs = "";

		while (cursor.moveToNext()) {
			keyValPairs += (cursor.getString(0) + "," + cursor.getString(1));
			keyValPairs += "~~";
		}

		if (keyValPairs == "") {
			keyValPairs = "~~";
		}

		return keyValPairs;
	}

	/* This function returns the cusrsor for a given key */
	private Cursor getSingleValue(String selection) {
		grpMsgDb = dbStorage.getReadableDatabase();

		String str = "select * from " + TABLE_NAME + " where " + DB_KEY + "='" + selection + "'";
		Cursor cursor = grpMsgDb.rawQuery(str, null);
		return cursor;
	}

	/* This function adds hashes ports to the port list*/
	private void initFunction() {
		try {
			portsArr = new String[] { "11108", "11112", "11116", "11120", "11124" };
			portHashSortList = new ArrayList<String>();
			portSuccMap = new HashMap<>();
			portHashMap = new HashMap<>();

			portHashSortList.add(genHash("5562"));
			portHashSortList.add(genHash("5556"));
			portHashSortList.add(genHash("5554"));
			portHashSortList.add(genHash("5558"));
			portHashSortList.add(genHash("5560"));

			portHashMap.put(genHash("5562"), "11124");
			portHashMap.put(genHash("5556"), "11112");
			portHashMap.put(genHash("5554"), "11108");
			portHashMap.put(genHash("5558"), "11116");
			portHashMap.put(genHash("5560"), "11120");

			portSuccMap.put("11124", "11112#11108");
			portSuccMap.put("11112", "11108#11116");
			portSuccMap.put("11108", "11116#11120");
			portSuccMap.put("11116", "11120#11124");
			portSuccMap.put("11120", "11124#11112");

			portPredMap = new HashMap<>();
			portPredMap.put("11124", "11120");
			portPredMap.put("11112", "11124");
			portPredMap.put("11108", "11112");
			portPredMap.put("11116", "11108");
			portPredMap.put("11120", "11116");

			queryMap = new HashMap<>();
			insertAckMap = new HashMap<>();

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "NoSuchAlgorithmException Exception");
		}
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	/*This is get the hash value of the input*/
	synchronized String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	static String TABLE_NAME = "GroupMsgTable";
	static int DB_VERSION = 2;

	/*This class is used to for Database i.e key,value storage */
	private static class DataStorage extends SQLiteOpenHelper {
		static String DB_NAME = "GroupDB";
		static String KEY = "key";
		static String VALUE = "value";
		String CREATE_DB_TABLE = " CREATE TABLE " + TABLE_NAME + " ( key TEXT NOT NULL UNIQUE, value TEXT NOT NULL);";

		DataStorage(Context context) {
			super(context, DB_NAME, null, DB_VERSION);
			Log.d("DataStorage", "db");
			Log.d("create", CREATE_DB_TABLE);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {

			db.execSQL(CREATE_DB_TABLE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL(TABLE_NAME);
			onCreate(db);
		}
	}
}

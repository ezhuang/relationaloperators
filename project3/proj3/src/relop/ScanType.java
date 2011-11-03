package relop;

public enum ScanType {

	FILESCAN, INDEXSCAN, KEYSCAN, HASHJOIN;

	public static ScanType getScanType(Object scanObj) {
		if (scanObj instanceof FileScan) {
			return FILESCAN;
		} else if (scanObj instanceof IndexScan) {
			return INDEXSCAN;
		} else if (scanObj instanceof KeyScan) {
			return KEYSCAN;
		} else if (scanObj instanceof HashJoin) {
			return HASHJOIN;
		}
		throw new UnsupportedOperationException("Invalid Scan type" + scanObj.getClass());

	}
	
}

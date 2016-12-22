package sample.hive.udf;

public class Feature implements Comparable<Feature>{
	private String key;
	private String value;
	private Long index;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Feature(Long index) {
		super();
		this.index = index;
	}
	
	public Feature(Integer index) {
		super();
		this.index = new Long(index);
	}
	
	public Feature(String key, Long index) {
		super();
		this.key = key;
		this.index = index;
	}
	
	public Feature(String key, Integer index) {
		super();
		this.key = key;
		this.index = new Long(index);
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Long getIndex() {
		return index;
	}

	public void setIndex(Long index) {
		this.index = index;
	}

	@Override
	public int compareTo(Feature o) {
		if(this.getIndex() == o.getIndex())
			return 0;
		else
			return new Long( this.getIndex() - o.getIndex() ).intValue();
	}

}

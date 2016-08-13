package com.axon.guolv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GuoLvBean implements Writable {
	// phone,terminal_id,start_time,end_time,AREANO,imei
	private String phone;
	private String terminalId;
	private String startTime;
	private String endTime;
	private String areaNo;
	private String imei;

	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeUTF(terminalId);
		out.writeUTF(startTime);
		out.writeUTF(endTime);
		out.writeUTF(areaNo);
		out.writeUTF(imei);

	}

	public void readFields(DataInput in) throws IOException {
		this.phone = in.readUTF();
		this.terminalId = in.readUTF();
		this.startTime = in.readUTF();
		this.endTime = in.readUTF();
		this.areaNo = in.readUTF();
		this.imei = in.readUTF();
	}

	@Override
	public String toString() {
		return "GuoLvBean [phone=" + phone + ", terminalId=" + terminalId
				+ ", startTime=" + startTime + ", endTime=" + endTime
				+ ", areaNo=" + areaNo + ", imei=" + imei + "]";
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getTerminalId() {
		return terminalId;
	}

	public void setTerminalId(String terminalId) {
		this.terminalId = terminalId;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getAreaNo() {
		return areaNo;
	}

	public void setAreaNo(String areaNo) {
		this.areaNo = areaNo;
	}

	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

}

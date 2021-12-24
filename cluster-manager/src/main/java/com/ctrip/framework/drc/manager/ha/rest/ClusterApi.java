package com.ctrip.framework.drc.manager.ha.rest;


import com.ctrip.framework.drc.manager.ha.cluster.ClusterServerInfo;

/**
 * @author wenchao.meng
 *
 * Jul 29, 2016
 */
public interface ClusterApi {

	String PATH_PREFIX = "/api/clustermanager";

	String PATH_NOTIFY_SLOT_CHANGE = "/notifyslotchange/{slotId}";
	String PATH_EXPORT_SLOT = "/exportslot/{slotId}";
	String PATH_IMPORT_SLOT = "/importslot/{slotId}";

	String PATH_ADD_SLOT = "/addslot/{slotId}";
	String PATH_DELETE_SLOT = "/deleteslot/{slotId}";

	String getServerId();
	
	ClusterServerInfo getClusterInfo();

	void addSlot(int slotId) throws Exception;
	
	void deleteSlot(int slotId) throws Exception;
	
	void exportSlot(int slotId) throws Exception;
	
	void importSlot(int slotId) throws Exception;

	void notifySlotChange(int slotId) throws Exception;

	String debug();
	
	void refresh() throws Exception;

}

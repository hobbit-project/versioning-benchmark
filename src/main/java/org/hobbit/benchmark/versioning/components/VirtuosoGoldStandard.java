/**
 * 
 */
package org.hobbit.benchmark.versioning.components;

import org.hobbit.core.components.AbstractPlatformConnectorComponent;
import org.hobbit.core.components.GeneratedDataReceivingComponent;
import org.hobbit.core.components.TaskReceivingComponent;

/**
 * @author papv
 *
 */
public class VirtuosoGoldStandard extends AbstractPlatformConnectorComponent
		implements GeneratedDataReceivingComponent, TaskReceivingComponent {

	/**
	 * 
	 */
	public VirtuosoGoldStandard() {
		super();
	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.Component#run()
	 */
	public void run() throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String taskId, byte[] data) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.GeneratedDataReceivingComponent#receiveGeneratedData(byte[])
	 */
	public void receiveGeneratedData(byte[] data) {
		// TODO Auto-generated method stub

	}

}

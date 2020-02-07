package org.one23lb.apim.event.consumer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.PartitionContext;

public interface EventWriter
{
	void write(final PartitionContext context, final EventData data);
}

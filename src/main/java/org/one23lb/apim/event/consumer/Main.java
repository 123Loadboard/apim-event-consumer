package org.one23lb.apim.event.consumer;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.one23lb.apim.event.core.Configuration;
import org.one23lb.apim.event.core.EventParser;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost.EventProcessorHostBuilder;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;
import com.microsoft.azure.eventprocessorhost.PartitionContext;

/**
 * Hello world!
 *
 */
public class Main
{
	private static final Logger LOG = Logger.getLogger(Main.class.getName());

	public static void main(final String[] args)
	{
		try
		{
			final Properties props = Configuration.get();
			final String namespaceName = props.getProperty("eventhub.ns");
			final String eventHubName = props.getProperty("eventhub.name");
			final String sasKeyName = props.getProperty("eventhub.sasKeyName");
			final String sasKey = props.getProperty("eventhub.sasKey");
			final String consumerGroup = props.getProperty("eventhub.consumerGroup");
			final String consumerName = props.getProperty("eventhub.consumerName", "eclipse");

			final String storageConnectionString = props.getProperty("storage.connectionString");
			final String storageContainerName = props.getProperty("storage.containerName");

			final String connStr = new ConnectionStringBuilder().setNamespaceName(namespaceName)
					.setEventHubName(eventHubName).setSasKeyName(sasKeyName).setSasKey(sasKey).toString();

			final EventProcessorHost host = EventProcessorHostBuilder
					.newBuilder(EventProcessorHost.createHostName(null), consumerGroup)
					.useAzureStorageCheckpointLeaseManager(storageConnectionString, storageContainerName, consumerName)
					.useEventHubConnectionString(connStr).build();

			LOG.info("Registering host named " + host.getHostName());

			if (args.length > 0)
			{
				LOG.info("Capturing to " + args[0]);

				try (final FileOutputStream fos = new FileOutputStream(args[0], true);
						final FileEventWriter writer = new FileEventWriter(fos))
				{
					EventProcessor.eventWriter = writer;

					process(host);
				}
			}
			else
			{
				process(host);
			}
		}
		catch (final Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void process(final EventProcessorHost host)
		throws Exception
	{
		EventProcessorOptions options = new EventProcessorOptions();
		options.setExceptionNotification(new ErrorNotificationHandler());

		host.registerEventProcessor(EventProcessor.class, options).whenComplete((unused, e) ->
		{
			if (e != null)
			{
				LOG.info("Failure while registering: " + e.toString());
				if (e.getCause() != null)
				{
					LOG.info("Inner exception: " + e.getCause().toString());
				}

				host.unregisterEventProcessor();
			}
		}).thenAccept((unused) ->
		{
			LOG.info("Press enter to stop.");
			try
			{
				System.in.read();
			}
			catch (Exception e)
			{
				LOG.info("Keyboard read failed: " + e.toString());
			}
		}).thenCompose((unused) ->
		{
			return host.unregisterEventProcessor();
		}).exceptionally((e) ->
		{
			LOG.info("Failure while unregistering: " + e.toString());
			if (e.getCause() != null)
			{
				LOG.info("Inner exception: " + e.getCause().toString());
			}
			return null;
		}).get(); // Wait for everything to finish before exiting main!

		LOG.info("End of sample");
	}

	private static final class ErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs>
	{
		@Override
		public void accept(ExceptionReceivedEventArgs t)
		{
			LOG.info("SAMPLE: Host " + t.getHostname() + " received general error notification during " + t.getAction()
					+ ": " + t.getException().toString());
		}
	}

	private static final class FileEventWriter implements EventWriter, AutoCloseable
	{
		private final Writer itsWriter;

		FileEventWriter(final OutputStream os)
		{
			itsWriter = new OutputStreamWriter(os, StandardCharsets.UTF_8);
		}

		@Override
		public void write(final PartitionContext context, final EventData data)
		{
			try
			{
				itsWriter.write("// " + context.getPartitionId() + "," + data.getSystemProperties().getOffset() + ","
						+ data.getSystemProperties().getSequenceNumber() + "\r\n");

				final String json = EventParser.parse(new String(data.getBytes(), StandardCharsets.UTF_8)).toString();

				itsWriter.write(json);
				itsWriter.write("\r\n");

				System.out.println(json);
			}
			catch (final Exception e)
			{
				LOG.log(Level.WARNING, "problem while capturing event:", e);
			}
		}

		@Override
		public void close() throws Exception
		{
			itsWriter.close();
		}
	}
}

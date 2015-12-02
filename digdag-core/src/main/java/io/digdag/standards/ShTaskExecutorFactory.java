package io.digdag.standards;

import java.util.Map;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import io.digdag.core.spi.CommandExecutor;
import io.digdag.core.spi.TaskRequest;
import io.digdag.core.spi.TaskExecutor;
import io.digdag.core.spi.TaskExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.config.Config;

public class ShTaskExecutorFactory
        implements TaskExecutorFactory
{
    private static Logger logger = LoggerFactory.getLogger(ShTaskExecutorFactory.class);

    private final CommandExecutor exec;

    @Inject
    public ShTaskExecutorFactory(CommandExecutor exec)
    {
        this.exec = exec;
    }

    public String getType()
    {
        return "sh";
    }

    public TaskExecutor newTaskExecutor(TaskRequest request)
    {
        return new ShTaskExecutor(request);
    }

    private class ShTaskExecutor
            extends BaseTaskExecutor
    {
        public ShTaskExecutor(TaskRequest request)
        {
            super(request);
        }

        @Override
        public Config runTask()
        {
            String command = request.getConfig().get("command", String.class);
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

            logger.info("sh>: {}", command);

            final Map<String, String> env = pb.environment();
            request.getParams().getKeys()
                .forEach(key -> {
                    JsonNode value = request.getParams().get(key, JsonNode.class);
                    String string;
                    if (value.isTextual()) {
                        string = value.textValue();
                    }
                    else {
                        string = value.toString();
                    }
                    env.put(key, string);
                });

            pb.redirectErrorStream(true);

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                Process p = exec.start(request, pb);
                p.getOutputStream().close();
                try (InputStream stdout = p.getInputStream()) {
                    ByteStreams.copy(stdout, buffer);
                }
                ecode = p.waitFor();
                message = buffer.toString();
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            //logger.info("Shell command message ===\n{}", message);  // TODO include task name
            System.out.println(message);
            if (ecode != 0) {
                throw new RuntimeException("Command failed: "+message);
            }

            return request.getParams().getFactory().create();
        }
    }
}

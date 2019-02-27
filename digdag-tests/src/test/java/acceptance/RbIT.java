package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.getAttemptLogs;
import static utils.TestUtils.main;

public class RbIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void verifyConfigurationPararms()
            throws Exception
    {
        final Path tempdir = folder.getRoot().toPath().toAbsolutePath();
        final Path projectDir = tempdir.resolve("echo_params");
        final Path scriptsDir = projectDir.resolve("scripts");

        // Create new project
        final CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));
        Files.createDirectories(scriptsDir);
        copyResource("acceptance/echo_params/echo_rb_params.dig", projectDir.resolve("echo_rb_params.dig"));
        copyResource("acceptance/echo_params/scripts/echo_params.rb", scriptsDir.resolve("echo_params.rb"));

        // Push the project
        final CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "echo_params",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        final CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "echo_params", "echo_rb_params",
                "--session", "now");
        assertThat(startStatus.code(), is(0));
        final Id attemptId = getAttemptId(startStatus);

        // Wait for the attempt to complete
        RestSessionAttempt attempt = null;
        for (int i = 0; i < 30; i++) {
            attempt = client.getSessionAttempt(attemptId);
            if (attempt.getDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(attempt.getSuccess(), is(true));

        final String logs = getAttemptLogs(client, attemptId);
        assertThat(logs, containsString("digdag params"));
    }
}

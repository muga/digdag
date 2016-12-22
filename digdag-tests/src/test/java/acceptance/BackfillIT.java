package acceptance;

import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSession;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.expect;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BackfillIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;
    private DigdagClient client;
    private Path outdir;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        outdir = projectDir.resolve("outdir");
    }

    /*
    @Test
    public void initPushBackfill()
            throws Exception
    {
        // Create new project
        {
            CommandStatus cmd = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(cmd.code(), is(0));
        }

        copyResource("acceptance/backfill/backfill.dig", projectDir.resolve("backfill.dig"));

        // Push
        {
            CommandStatus cmd = main("push",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectDir.toString(),
                    "backfill-test");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        // Backfill the workflow
        {
            CommandStatus cmd = main("backfill",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "backfill-test", "backfill",
                    "--from", "2016-01-01",
                    "--count", "2");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        // Verify that 2 sessions are started
        List<RestSession> sessions = client.getSessions().getSessions();
        assertThat(sessions.size(), is(2));

        // sessions API return results in reversed order

        RestSession session1 = sessions.get(1);
        assertThat(session1.getSessionTime(), is(OffsetDateTime.parse("2016-01-01T00:00:00+09:00")));

        RestSession session2 = sessions.get(0);
        assertThat(session2.getSessionTime(), is(OffsetDateTime.parse("2016-01-02T00:00:00+09:00")));
    }
    */

    @Test
    public void backfillSequentially()
            throws Exception
    {
        // Create new project
        {
            CommandStatus cmd = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(cmd.code(), is(0));
        }

        copyResource("acceptance/backfill/backfill_sequential.dig", projectDir.resolve("backfill_sequential.dig"));

        // Push
        {
            CommandStatus cmd = main("push",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "-p", "outdir="+outdir.toString(),
                    "--project", projectDir.toString(),
                    "backfill-test");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        Files.createDirectories(outdir);

        // Backfill the workflow
        {
            CommandStatus cmd = main("backfill",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "backfill-test", "backfill_sequential",
                    "--from", "2016-01-01",
                    "--count", "3");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        // Verify that 3 sessions are started
        List<RestSession> sessions = client.getSessions().getSessions();
        assertThat(sessions.size(), is(3));

        for (RestSession session : sessions) {
            expect(Duration.ofMinutes(5), attemptSuccess(server.endpoint(), session.getLastAttempt().get().getId()));
        }

        // sessions API return results in reversed order

        String r1 = new String(Files.readAllBytes(outdir.resolve("runtime_20160101.txt")), UTF_8);
        String r2 = new String(Files.readAllBytes(outdir.resolve("runtime_20160102.txt")), UTF_8);
        String r3 = new String(Files.readAllBytes(outdir.resolve("runtime_20160103.txt")), UTF_8);

        String e1 = new String(Files.readAllBytes(outdir.resolve("last_executed_20160101.txt")), UTF_8);
        String e2 = new String(Files.readAllBytes(outdir.resolve("last_executed_20160102.txt")), UTF_8);
        String e3 = new String(Files.readAllBytes(outdir.resolve("last_executed_20160103.txt")), UTF_8);
    }
}

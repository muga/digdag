package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionCollection;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptCollection;
import io.digdag.client.config.Config;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionWithLastAttempt;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.SessionTarget;
import io.digdag.spi.ac.SiteTarget;
import io.swagger.annotations.Api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.util.List;
import java.util.stream.Collectors;

@Api("Session")
@Path("/")
@Produces("application/json")
public class SessionResource
        extends AuthenticatedResource
{
    // GET  /api/sessions                                    # List sessions from recent to old
    // GET  /api/sessions/{id}                               # Get a session by id
    // GET  /api/sessions/{id}/attempts                      # List attempts of a session

    private final ProjectStoreManager rm;
    private final SessionStoreManager sm;
    private final TransactionManager tm;
    private final AccessController ac;
    private static int MAX_SESSIONS_PAGE_SIZE;
    private static final int DEFAULT_SESSIONS_PAGE_SIZE = 100;
    private static int MAX_ATTEMPTS_PAGE_SIZE;
    private static final int DEFAULT_ATTEMPTS_PAGE_SIZE = 100;

    @Inject
    public SessionResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TransactionManager tm,
            AccessController ac,
            Config systemConfig)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.ac = ac;
        MAX_SESSIONS_PAGE_SIZE = systemConfig.get("api.max_sessions_page_size", Integer.class, DEFAULT_SESSIONS_PAGE_SIZE);
        MAX_ATTEMPTS_PAGE_SIZE = systemConfig.get("api.max_attempts_page_size", Integer.class, DEFAULT_ATTEMPTS_PAGE_SIZE);
    }

    @GET
    @Path("/api/sessions")
    public RestSessionCollection getSessions(
            @QueryParam("last_id") Long lastId,
            @QueryParam("page_size") Integer pageSize)
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_SESSIONS_PAGE_SIZE, DEFAULT_SESSIONS_PAGE_SIZE);

        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            List<StoredSessionWithLastAttempt> sessions = ss.getSessions(validPageSize, Optional.fromNullable(lastId),
                    () -> ac.getListSessionsFilterOf(
                            SiteTarget.of(getSiteId()),
                            getUserInfo())
            );

            return RestModels.sessionCollection(rs, sessions);
        });
    }

    @GET
    @Path("/api/sessions/{id}")
    public RestSession getSession(@PathParam("id") long id)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            StoredSessionWithLastAttempt session = sm.getSessionStore(getSiteId())
                    .getSessionById(id, // NotFound
                            () -> ac.getGetSessionFilterOf(
                                    SiteTarget.of(getSiteId()),
                                    getUserInfo())
                    );

            StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(session.getProjectId(), // NotFound
                            () -> ac.getGetProjectFilterOf( // TODO need to revisit to decide that we can use getGetProjectFilter method
                                    SiteTarget.of(getSiteId()),
                                    getUserInfo())
                    );

            return RestModels.session(session, proj.getName());
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/sessions/{id}/attempts")
    public RestSessionAttemptCollection getSessionAttempts(
            @PathParam("id") long id,
            @QueryParam("last_id") Long lastId,
            @QueryParam("page_size") Integer pageSize)
            throws ResourceNotFoundException
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_ATTEMPTS_PAGE_SIZE, DEFAULT_ATTEMPTS_PAGE_SIZE);

        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            final StoredSession session = ss.getSessionById(id, // NotFound
                    () -> ac.getListSessionAttemptsFilterOf(
                            SiteTarget.of(getSiteId()),
                            getUserInfo())
            );
            final StoredProject project = rs.getProjectById(session.getProjectId(), // NotFound
                    () -> ac.getGetProjectFilterOf( // TODO need to revisit to decide that we can use getGetProjectFilter method
                            SiteTarget.of(getSiteId()),
                            getUserInfo())
            );
            List<StoredSessionAttempt> attempts = ss.getAttemptsOfSession(id, validPageSize, Optional.fromNullable(lastId),
                    () -> ac.getListSessionAttemptsFilterOf(
                            SessionTarget.of(getSiteId(),
                                    session.getId(),
                                    session.getWorkflowName(),
                                    project.getId(),
                                    project.getName()),
                            getUserInfo()
                    )
            );

            List<RestSessionAttempt> collection = attempts.stream()
                    .map(attempt -> RestModels.attempt(session, attempt, project.getName()))
                    .collect(Collectors.toList());

            return RestModels.attemptCollection(collection);
        }, ResourceNotFoundException.class);
    }
}

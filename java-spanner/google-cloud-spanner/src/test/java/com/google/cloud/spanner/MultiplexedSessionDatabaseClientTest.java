/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.NoCredentials;
import com.google.cloud.grpc.GrpcTransportOptions.ExecutorFactory;
import com.google.cloud.spanner.SessionClient.SessionConsumer;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class MultiplexedSessionDatabaseClientTest {
  @After
  public void tearDown() throws Exception {
    clearChannelUsage();
  }

  @Test
  public void testMaintainer() {
    // This fails for the native builds due to the extensive use of reflection.
    assumeTrue(isJava8());

    Instant now = Instant.now();
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(now);
    SessionClient sessionClient = mock(SessionClient.class);
    SpannerImpl spanner = mock(SpannerImpl.class);
    SpannerOptions spannerOptions = mock(SpannerOptions.class);
    SessionPoolOptions sessionPoolOptions = mock(SessionPoolOptions.class);
    when(sessionClient.getSpanner()).thenReturn(spanner);
    when(spanner.getOptions()).thenReturn(spannerOptions);
    when(spannerOptions.getSessionPoolOptions()).thenReturn(sessionPoolOptions);
    when(sessionPoolOptions.getMultiplexedSessionMaintenanceDuration())
        .thenReturn(Duration.ofDays(7));
    when(sessionPoolOptions.getMultiplexedSessionMaintenanceLoopFrequency())
        .thenReturn(Duration.ofMinutes(10));

    SessionImpl session1 = mock(SessionImpl.class);
    SessionReference sessionReference1 = mock(SessionReference.class);
    when(session1.getSessionReference()).thenReturn(sessionReference1);

    SessionImpl session2 = mock(SessionImpl.class);
    SessionReference sessionReference2 = mock(SessionReference.class);
    when(session2.getSessionReference()).thenReturn(sessionReference2);

    doAnswer(
            (Answer<?>)
                invocationOnMock -> {
                  SessionConsumer consumer = invocationOnMock.getArgument(0);
                  // Return session1 the first time it is called.
                  consumer.onSessionReady(session1);
                  return null;
                })
        .doAnswer(
            (Answer<?>)
                invocationOnMock -> {
                  SessionConsumer consumer = invocationOnMock.getArgument(0);
                  // Return session2 the second time that it is called.
                  consumer.onSessionReady(session2);
                  return null;
                })
        .when(sessionClient)
        .asyncCreateMultiplexedSession(any(SessionConsumer.class));

    // Create a client. This should get session1.
    MultiplexedSessionDatabaseClient client =
        new MultiplexedSessionDatabaseClient(sessionClient, clock);

    // Make sure that the client uses the initial session that is created.
    assertEquals(client.getCurrentSessionReference(), session1.getSessionReference());

    // Run the maintainer without advancing the clock. We should still get the same session.
    client.getMaintainer().maintain();
    assertEquals(client.getCurrentSessionReference(), session1.getSessionReference());

    // Advance the clock by 1 day. We should still get the same session.
    when(clock.instant()).thenReturn(now.plus(Duration.ofDays(1)));
    client.getMaintainer().maintain();
    assertEquals(client.getCurrentSessionReference(), session1.getSessionReference());

    // Advance the clock by 8 days. We should now get a new session.
    when(clock.instant()).thenReturn(now.plus(Duration.ofDays(8)));
    client.getMaintainer().maintain();
    assertEquals(client.getCurrentSessionReference(), session2.getSessionReference());
  }

  @Test
  public void testDisableMultiplexedSessionEnvVar() throws Exception {
    assumeTrue(isJava8() && !isWindows());
    assumeFalse(System.getenv().containsKey("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS"));

    // Assert that the mux sessions setting is respected by default.
    assertTrue(
        SessionPoolOptions.newBuilder()
            .setUseMultiplexedSession(true)
            .build()
            .getUseMultiplexedSession());

    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeableEnvironmentVariables =
        (Map<String, String>) field.get(System.getenv());

    try {
      writeableEnvironmentVariables.put("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "false");
      // Assert that the env var overrides the mux sessions setting.
      assertFalse(
          SessionPoolOptions.newBuilder()
              .setUseMultiplexedSession(true)
              .build()
              .getUseMultiplexedSession());
    } finally {
      writeableEnvironmentVariables.remove("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS");
    }
  }

  @Test
  public void testEnableMultiplexedSessionEnvVar() throws Exception {
    assumeTrue(isJava8() && !isWindows());
    assumeFalse(System.getenv().containsKey("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS"));

    // Assert that the mux sessions setting is respected by default.
    assertFalse(
        SessionPoolOptions.newBuilder()
            .setUseMultiplexedSession(false)
            .build()
            .getUseMultiplexedSession());

    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeableEnvironmentVariables =
        (Map<String, String>) field.get(System.getenv());

    try {
      writeableEnvironmentVariables.put("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "true");
      // Assert that the env var overrides the mux sessions setting.
      assertTrue(
          SessionPoolOptions.newBuilder()
              .setUseMultiplexedSession(false)
              .build()
              .getUseMultiplexedSession());
    } finally {
      writeableEnvironmentVariables.remove("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS");
    }
  }

  @Test
  public void testIgnoreMultiplexedSessionEnvVar() throws Exception {
    assumeTrue(isJava8() && !isWindows());
    assumeFalse(System.getenv().containsKey("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS"));

    // Assert that the mux sessions setting is respected by default.
    assertFalse(
        SessionPoolOptions.newBuilder()
            .setUseMultiplexedSession(false)
            .build()
            .getUseMultiplexedSession());

    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeableEnvironmentVariables =
        (Map<String, String>) field.get(System.getenv());

    try {
      writeableEnvironmentVariables.put("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "");
      // Assert that the env var overrides the mux sessions setting.
      assertFalse(
          SessionPoolOptions.newBuilder()
              .setUseMultiplexedSession(false)
              .build()
              .getUseMultiplexedSession());
    } finally {
      writeableEnvironmentVariables.remove("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS");
    }
  }

  @Test
  public void testThrowExceptionMultiplexedSessionEnvVarInvalidValues() throws Exception {
    assumeTrue(isJava8() && !isWindows());
    assumeFalse(System.getenv().containsKey("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS"));

    // Assert that the mux sessions setting is respected by default.
    assertFalse(
        SessionPoolOptions.newBuilder()
            .setUseMultiplexedSession(false)
            .build()
            .getUseMultiplexedSession());

    Class<?> classOfMap = System.getenv().getClass();
    Field field = classOfMap.getDeclaredField("m");
    field.setAccessible(true);
    Map<String, String> writeableEnvironmentVariables =
        (Map<String, String>) field.get(System.getenv());

    try {
      writeableEnvironmentVariables.put("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS", "test");

      // setting an invalid GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS value throws error.
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  SessionPoolOptions.newBuilder()
                      .setUseMultiplexedSession(false)
                      .build()
                      .getUseMultiplexedSession());
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      assertThat(sw.toString())
          .contains("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS should be either true or false");
    } finally {
      writeableEnvironmentVariables.remove("GOOGLE_CLOUD_SPANNER_MULTIPLEXED_SESSIONS");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGrpcGcpSingleUseDoesNotReserveBitsetChannelHint() throws Exception {
    assumeTrue(isJava8());
    SessionClient sessionClient = mock(SessionClient.class);
    SpannerImpl spanner = mock(SpannerImpl.class);
    SpannerOptions spannerOptions = mock(SpannerOptions.class);
    SessionPoolOptions sessionPoolOptions = mock(SessionPoolOptions.class);
    TraceWrapper tracer = mock(TraceWrapper.class);
    ISpan span = mock(ISpan.class);

    when(sessionClient.getSpanner()).thenReturn(spanner);
    when(spanner.getOptions()).thenReturn(spannerOptions);
    when(spanner.getTracer()).thenReturn(tracer);
    when(tracer.getCurrentSpan()).thenReturn(span);
    when(spannerOptions.getNumChannels()).thenReturn(4);
    when(spannerOptions.isGrpcGcpExtensionEnabled()).thenReturn(true);
    when(spannerOptions.getSessionPoolOptions()).thenReturn(sessionPoolOptions);
    when(sessionPoolOptions.getMultiplexedSessionMaintenanceDuration())
        .thenReturn(Duration.ofDays(7));
    when(sessionPoolOptions.getWaitForMinSessions()).thenReturn(Duration.ZERO);

    MultiplexedSessionDatabaseClient client =
        new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());
    SessionReference sessionReference =
        new SessionReference(
            "projects/p/instances/i/databases/d/sessions/s1",
            null,
            com.google.protobuf.Timestamp.getDefaultInstance(),
            true,
            null);

    Field sessionFutureField =
        MultiplexedSessionDatabaseClient.class.getDeclaredField("multiplexedSessionReference");
    sessionFutureField.setAccessible(true);
    @SuppressWarnings("unchecked")
    AtomicReference<com.google.api.core.ApiFuture<SessionReference>> sessionFutureRef =
        (AtomicReference<com.google.api.core.ApiFuture<SessionReference>>)
            sessionFutureField.get(client);
    sessionFutureRef.set(ApiFutures.immediateFuture(sessionReference));

    java.lang.reflect.Method method =
        MultiplexedSessionDatabaseClient.class.getDeclaredMethod(
            "createDirectMultiplexedSessionTransaction", boolean.class);
    method.setAccessible(true);
    method.invoke(client, true);

    Field field =
        MultiplexedSessionDatabaseClient.class.getDeclaredField("numCurrentSingleUseTransactions");
    field.setAccessible(true);
    AtomicInteger counter = (AtomicInteger) field.get(client);
    assertEquals(0, counter.get());
  }

  @Test
  public void testCloseRemovesChannelUsageEntryWhenLastClientCloses() throws Exception {
    try (SpannerImpl spanner = createTestSpanner();
        SessionClient sessionClient = createSessionClient(spanner)) {
      MultiplexedSessionDatabaseClient client =
          new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());

      assertEquals(1, getChannelUsage().size());

      client.close();

      assertEquals(0, getChannelUsage().size());
    }
  }

  @Test
  public void testCloseKeepsChannelUsageEntryWhileAnotherClientIsUsingSameSpanner()
      throws Exception {
    try (SpannerImpl spanner = createTestSpanner();
        SessionClient firstSessionClient = createSessionClient(spanner);
        SessionClient secondSessionClient = createSessionClient(spanner)) {
      MultiplexedSessionDatabaseClient firstClient =
          new MultiplexedSessionDatabaseClient(firstSessionClient, Clock.systemUTC());
      MultiplexedSessionDatabaseClient secondClient =
          new MultiplexedSessionDatabaseClient(secondSessionClient, Clock.systemUTC());

      assertEquals(1, getChannelUsage().size());

      firstClient.close();
      assertEquals(1, getChannelUsage().size());

      secondClient.close();
      assertEquals(0, getChannelUsage().size());
    }
  }

  @Test
  public void testChannelUsageDoesNotPinSpannerThatIsNotClosed() throws Exception {
    assertEquals(0, getChannelUsage().size());

    SpannerImpl spanner = createTestSpanner();
    SessionClient sessionClient = createSessionClient(spanner);
    MultiplexedSessionDatabaseClient client =
        new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());
    assertEquals(1, getChannelUsage().size());

    // Drop all references to the client and the Spanner instance *without* closing them. The
    // CHANNEL_USAGE map is keyed weakly, so it must not keep the Spanner instance (and with it the
    // entire client graph) alive.
    WeakReference<SpannerImpl> spannerReference = new WeakReference<>(spanner);
    client = null;
    sessionClient = null;
    spanner = null;

    for (int attempt = 0; attempt < 100 && spannerReference.get() != null; attempt++) {
      System.gc();
      Thread.sleep(10L);
    }

    assertNull(spannerReference.get());
    assertEquals(0, getChannelUsage().size());
  }

  @Test
  public void testSessionReadySchedulesMaintainer() throws Exception {
    try (SpannerImpl spanner = createTestSpanner();
        DeferredMultiplexedSessionClient sessionClient =
            new DeferredMultiplexedSessionClient(spanner)) {
      MultiplexedSessionDatabaseClient client =
          new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());
      // The CreateSession RPC is still in flight, so no maintenance task has been scheduled yet.
      assertNull(getScheduledFuture(client));

      sessionClient.completeSessionCreation();
      ScheduledFuture<?> scheduledFuture = getScheduledFuture(client);
      assertNotNull(scheduledFuture);
      assertFalse(scheduledFuture.isCancelled());

      client.close();
      assertTrue(scheduledFuture.isCancelled());
    }
  }

  @Test
  public void testMaintainerIsOnlyScheduledOnce() throws Exception {
    try (SpannerImpl spanner = createTestSpanner();
        DeferredMultiplexedSessionClient sessionClient =
            new DeferredMultiplexedSessionClient(spanner)) {
      MultiplexedSessionDatabaseClient client =
          new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());

      sessionClient.completeSessionCreation();
      ScheduledFuture<?> scheduledFuture = getScheduledFuture(client);
      assertNotNull(scheduledFuture);

      // A second call must not schedule a second task, as only the last scheduled future is
      // retained and can be cancelled by close().
      sessionClient.completeSessionCreation();
      assertSame(scheduledFuture, getScheduledFuture(client));

      client.close();
      assertTrue(scheduledFuture.isCancelled());
    }
  }

  @Test
  public void testCloseBeforeSessionReadyDoesNotScheduleMaintainer() throws Exception {
    try (SpannerImpl spanner = createTestSpanner();
        DeferredMultiplexedSessionClient sessionClient =
            new DeferredMultiplexedSessionClient(spanner)) {
      MultiplexedSessionDatabaseClient client =
          new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());

      // Close the client while the initial CreateSession RPC is still in flight. maintainer.stop()
      // has nothing to cancel at this point.
      client.close();

      // The CreateSession RPC now completes. This must not schedule a maintenance task, as nothing
      // would ever cancel it again.
      sessionClient.completeSessionCreation();

      assertNull(getScheduledFuture(client));
    }
  }

  @Test
  public void testMaintainerTaskCancelsItselfWhenMaintainerIsCollected() throws Exception {
    try (SpannerImpl spanner = createTestSpanner();
        SessionClient sessionClient = createSessionClient(spanner)) {
      MultiplexedSessionDatabaseClient client =
          new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());

      ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
      MultiplexedSessionDatabaseClient.MaintainerTask task =
          new MultiplexedSessionDatabaseClient.MaintainerTask(client.getMaintainer());
      task.setScheduledFuture(scheduledFuture);

      // As long as the maintainer is reachable, the task just runs the maintenance. The session has
      // not expired, so this is a no-op.
      task.run();
      verify(scheduledFuture, never()).cancel(anyBoolean());

      // Clear the weak reference to simulate the client being garbage collected without being
      // closed. The task must then cancel itself, so that the static executor stops running it.
      clearMaintainerReference(task);
      task.run();
      verify(scheduledFuture).cancel(false);
    }
  }

  @Test
  public void testUseAfterCloseThrows() {
    try (SpannerImpl spanner = createTestSpanner();
        SessionClient sessionClient = createSessionClient(spanner)) {
      MultiplexedSessionDatabaseClient client =
          new MultiplexedSessionDatabaseClient(sessionClient, Clock.systemUTC());
      client.close();

      assertThrows(IllegalStateException.class, client::singleUse);
    }
  }

  @Test
  public void testIsClosedIsVolatile() throws Exception {
    // isClosed is written under synchronized(this) in close(), but read without any synchronization
    // in createMultiplexedSessionTransaction(..). It must therefore be volatile for that read to be
    // guaranteed to observe the close.
    Field field = MultiplexedSessionDatabaseClient.class.getDeclaredField("isClosed");
    assertTrue(Modifier.isVolatile(field.getModifiers()));
  }

  private ScheduledFuture<?> getScheduledFuture(MultiplexedSessionDatabaseClient client)
      throws Exception {
    Field field =
        MultiplexedSessionDatabaseClient.MultiplexedSessionMaintainer.class.getDeclaredField(
            "scheduledFuture");
    field.setAccessible(true);
    return (ScheduledFuture<?>) field.get(client.getMaintainer());
  }

  private void clearMaintainerReference(MultiplexedSessionDatabaseClient.MaintainerTask task)
      throws Exception {
    Field field =
        MultiplexedSessionDatabaseClient.MaintainerTask.class.getDeclaredField(
            "maintainerReference");
    field.setAccessible(true);
    ((WeakReference<?>) field.get(task)).clear();
  }

  private SessionClient createSessionClient(SpannerImpl spanner) {
    return new FailingMultiplexedSessionClient(spanner);
  }

  private SpannerImpl createTestSpanner() {
    SessionPoolOptions sessionPoolOptions =
        SessionPoolOptions.newBuilder()
            .setMultiplexedSessionMaintenanceDuration(Duration.ofDays(7))
            .setWaitForMinSessionsDuration(Duration.ZERO)
            .build();
    SpannerOptions options =
        SpannerOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .setNumChannels(4)
            .setSessionPoolOption(sessionPoolOptions)
            .build();
    return new SpannerImpl(options);
  }

  @SuppressWarnings("unchecked")
  private Map<?, ?> getChannelUsage() throws Exception {
    Field field = MultiplexedSessionDatabaseClient.class.getDeclaredField("CHANNEL_USAGE");
    field.setAccessible(true);
    return (Map<?, ?>) field.get(null);
  }

  private void clearChannelUsage() throws Exception {
    getChannelUsage().clear();
  }

  private boolean isJava8() {
    return JavaVersionUtil.getJavaMajorVersion() == 8;
  }

  private boolean isWindows() {
    return System.getProperty("os.name").toLowerCase().contains("windows");
  }

  private static final class TestExecutorFactory
      implements ExecutorFactory<ScheduledExecutorService> {
    @Override
    public ScheduledExecutorService get() {
      return Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void release(ScheduledExecutorService executor) {
      executor.shutdown();
      try {
        executor.awaitTermination(10L, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * {@link SessionClient} that does not complete the CreateSession RPC until {@link
   * #completeSessionCreation()} is called. This allows tests to interleave {@link
   * MultiplexedSessionDatabaseClient#close()} with the completion of the initial session creation.
   */
  private static final class DeferredMultiplexedSessionClient extends SessionClient {
    private static final DatabaseId TEST_DATABASE_ID =
        DatabaseId.of("test-project", "test-instance", "test-database");

    private SessionConsumer consumer;

    private DeferredMultiplexedSessionClient(SpannerImpl spanner) {
      super(spanner, TEST_DATABASE_ID, new TestExecutorFactory());
    }

    @Override
    void asyncCreateMultiplexedSession(SessionConsumer consumer) {
      this.consumer = consumer;
    }

    void completeSessionCreation() {
      SessionImpl session = mock(SessionImpl.class);
      when(session.getSessionReference()).thenReturn(mock(SessionReference.class));
      this.consumer.onSessionReady(session);
    }
  }

  private static final class FailingMultiplexedSessionClient extends SessionClient {
    private static final DatabaseId TEST_DATABASE_ID =
        DatabaseId.of("test-project", "test-instance", "test-database");

    private FailingMultiplexedSessionClient(SpannerImpl spanner) {
      super(spanner, TEST_DATABASE_ID, new TestExecutorFactory());
    }

    @Override
    void asyncCreateMultiplexedSession(SessionConsumer consumer) {
      consumer.onSessionCreateFailure(
          SpannerExceptionFactory.newSpannerException(ErrorCode.UNAUTHENTICATED, "test"), 1);
    }
  }
}

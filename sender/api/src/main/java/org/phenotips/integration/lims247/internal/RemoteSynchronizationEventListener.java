/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.phenotips.integration.lims247.internal;

import org.phenotips.Constants;
import org.phenotips.data.events.PatientChangedEvent;
import org.phenotips.data.events.PatientDeletedEvent;

import org.xwiki.component.annotation.Component;
import org.xwiki.component.phase.Initializable;
import org.xwiki.configuration.ConfigurationSource;
import org.xwiki.context.Execution;
import org.xwiki.model.reference.DocumentReference;
import org.xwiki.observation.AbstractEventListener;
import org.xwiki.observation.event.Event;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;

import com.xpn.xwiki.XWiki;
import com.xpn.xwiki.XWikiContext;
import com.xpn.xwiki.XWikiException;
import com.xpn.xwiki.doc.XWikiDocument;
import com.xpn.xwiki.objects.BaseObject;
import com.xpn.xwiki.util.AbstractXWikiRunnable;

/**
 * Pushes updated (or deleted) patient records to remote PhenoTips instances.
 *
 * @version $Id$
 */
@Component
@Named("pushDataToPublicClone")
@Singleton
public class RemoteSynchronizationEventListener extends AbstractEventListener implements Initializable
{
    /** The content type of the data sent in a request. */
    private static final ContentType REQUEST_CONTENT_TYPE = ContentType.create(
        ContentType.APPLICATION_XML.getMimeType(), Consts.UTF_8);

    /** The configuration property holding the request timeout duration. */
    private static final String CONFIG_REQ_TIMEOUT_PROPNAME = "phenotips.remoteSynchronization.requestTimeoutSecs";

    /** The name of the thread used for remote synchronization. */
    private static final String REMOTE_SYNC_THREAD_NAME = "PhenoTips remote synchronization request";

    /** Logging helper object. */
    @Inject
    private Logger logger;

    /** Provides access to the current request context. */
    @Inject
    private Execution execution;

    /** Provides access to configuration file values. */
    @Inject
    @Named("xwikiproperties")
    private ConfigurationSource configuration;

    /** HTTP client used for communicating with the remote server. */
    private final CloseableHttpClient client = HttpClients.createSystem();

    /** Thread executor used for asynchronous requests. */
    private ExecutorService executor;

    /** Holds HTTP request configuration to be used for different HTTP requests. */
    private RequestConfig requestConfig;

    /**
     * Thread for individual asynchronous remote synchronization requests.
     */
    private class RemoteSynchronizationRequestThread extends AbstractXWikiRunnable {
        /** Logging helper object. */
        private Logger logger;

        /** HTTP client used for communicating with the remote server. */
        private final CloseableHttpClient client;

        /** Request to send to the remote server. */
        private final HttpPost request;

        RemoteSynchronizationRequestThread(Logger logger, CloseableHttpClient client, HttpPost request) {
            this.logger = logger;
            this.client = client;
            this.request = request;
        }

        @Override
        public void runInternal() {
            this.logger.debug("Sending patient clone request to [{}]", request.getURI());
            try {
                this.client.execute(this.request).close();
                this.logger.debug("Finished patient clone request to [{}]", request.getURI());
            } catch (Exception ex) {
                this.logger.warn("Patient clone request failed: {}", ex.getMessage(), ex);
            } finally {
                if (request != null) {
                    request.releaseConnection();
                }
            }
        }
    }

    /** Default constructor, sets up the listener name and the list of events to subscribe to. */
    public RemoteSynchronizationEventListener()
    {
        super("pushDataToPublicClone", new PatientChangedEvent(), new PatientDeletedEvent());
    }

    @Override
    public void initialize()
    {
        String timeoutSecsVal = configuration.getProperty(CONFIG_REQ_TIMEOUT_PROPNAME);
        Integer timeoutSecs = NumberUtils.createInteger(timeoutSecsVal);
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        if (timeoutSecs != null && timeoutSecs >= 0) {
            requestConfigBuilder.setConnectTimeout(timeoutSecs * 1000);
            requestConfigBuilder.setConnectionRequestTimeout(timeoutSecs * 1000);
            requestConfigBuilder.setSocketTimeout(timeoutSecs * 1000);
        }
        this.requestConfig = requestConfigBuilder.build();

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
            .namingPattern(this.REMOTE_SYNC_THREAD_NAME)
            .daemon(false)
            .priority(Thread.NORM_PRIORITY)
            .build();
        this.executor = Executors.newSingleThreadExecutor(factory);
    }

    @Override
    public void onEvent(Event event, Object source, Object data)
    {
        if (event instanceof PatientDeletedEvent) {
            handleDelete((XWikiDocument) source);
        } else {
            handleUpdate((XWikiDocument) source);
        }
    }

    private void handleUpdate(XWikiDocument doc)
    {
        try {
            this.logger.debug("Pushing updated document [{}]", doc.getDocumentReference());
            XWikiContext context = getXContext();
            String payload = doc.toXML(true, false, true, false, context);
            List<BaseObject> servers = getRegisteredServers(context);
            if (servers != null && !servers.isEmpty()) {
                for (BaseObject serverConfiguration : servers) {
                    submitData(payload, serverConfiguration);
                }
            }
        } catch (XWikiException ex) {
            this.logger.warn("Failed to serialize changed document: {}", ex.getMessage());
        }
    }

    private void handleDelete(XWikiDocument doc)
    {
        this.logger.debug("Pushing deleted document [{}]", doc.getDocumentReference());
        XWikiContext context = getXContext();
        List<BaseObject> servers = getRegisteredServers(context);
        if (servers != null && !servers.isEmpty()) {
            for (BaseObject serverConfiguration : servers) {
                deleteData(doc.getDocumentReference().toString(), serverConfiguration);
            }
        }
    }

    /**
     * Get all the trusted remote instances where data should be sent that are configured in the current instance.
     *
     * @param context the current request object
     * @return a list of {@link BaseObject XObjects} with LIMS server configurations, may be {@code null}
     */
    private List<BaseObject> getRegisteredServers(XWikiContext context)
    {
        try {
            XWiki xwiki = context.getWiki();
            XWikiDocument prefsDoc =
                xwiki.getDocument(new DocumentReference(xwiki.getDatabase(), "XWiki", "XWikiPreferences"), context);
            return prefsDoc
                .getXObjects(new DocumentReference(xwiki.getDatabase(), Constants.CODE_SPACE, "RemoteClone"));
        } catch (XWikiException ex) {
            return Collections.emptyList();
        }
    }

    /**
     * Send the changed document to a remote PhenoTips instance.
     *
     * @param doc the serialized document to send
     * @param serverConfiguration the XObject holding the remote server configuration
     */
    private void submitData(String doc, BaseObject serverConfiguration)
    {
        HttpPost request = null;
        try {
            String submitURL = getSubmitURL(serverConfiguration);
            if (StringUtils.isNotBlank(submitURL)) {
                this.logger.debug("Queueing patient clone to remote server [{}]", submitURL);
                request = new HttpPost(submitURL);
                request.setEntity(new StringEntity(doc, REQUEST_CONTENT_TYPE));
                request.setConfig(this.requestConfig);

                RemoteSynchronizationRequestThread remoteSynchronizationRequestThread =
                    new RemoteSynchronizationRequestThread(this.logger, this.client, request);
                executor.execute(remoteSynchronizationRequestThread);
            }
        } catch (Exception ex) {
            this.logger.warn("Failed to queue patient clone to remote server: {}", ex.getMessage(), ex);
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
        }
    }

    /**
     * Notify a remote PhenoTips instance of a deleted document.
     *
     * @param doc the name of the deleted document
     * @param serverConfiguration the XObject holding the remote server configuration
     */
    private void deleteData(String doc, BaseObject serverConfiguration)
    {
        HttpPost request = null;
        try {
            String deleteURL = getDeleteURL(serverConfiguration);
            if (StringUtils.isNotBlank(deleteURL)) {
                this.logger.debug("Queueing patient deletion to remote server [{}]", deleteURL);
                request = new HttpPost(deleteURL);
                NameValuePair data = new BasicNameValuePair("document", doc);
                request.setEntity(new UrlEncodedFormEntity(Collections.singletonList(data), Consts.UTF_8));
                request.setConfig(this.requestConfig);

                RemoteSynchronizationRequestThread remoteSynchronizationRequestThread =
                    new RemoteSynchronizationRequestThread(this.logger, this.client, request);
                executor.execute(remoteSynchronizationRequestThread);
            }
        } catch (Exception ex) {
            this.logger.warn("Failed to queue patient removal request to remote server: {}", ex.getMessage(), ex);
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
        }
    }

    /**
     * Return the URL of the specified remote PhenoTips instance, where the updated document should be sent.
     *
     * @param serverConfiguration the XObject holding the remote server configuration
     * @return the configured URL, in the format {@code http://remote.host.name/bin/receive/data/}, or {@code null} if
     *         the configuration isn't valid
     */
    private String getSubmitURL(BaseObject serverConfiguration)
    {
        String result = getBaseURL(serverConfiguration);
        if (StringUtils.isNotBlank(result)) {
            return result + "?action=save&token=" + getToken(serverConfiguration);
        }
        return null;
    }

    /**
     * Return the URL of the specified remote PhenoTips instance, where the notifications about deleted documents should
     * be sent.
     *
     * @param serverConfiguration the XObject holding the remote server configuration
     * @return the configured URL, in the format {@code http://remote.host.name/bin/deleted/data/}, or {@code null} if
     *         the configuration isn't valid
     */
    private String getDeleteURL(BaseObject serverConfiguration)
    {
        String result = getBaseURL(serverConfiguration);
        if (StringUtils.isNotBlank(result)) {
            return result + "?action=delete&token=" + getToken(serverConfiguration);
        }
        return null;
    }

    /**
     * Return the base URL of the specified remote PhenoTips instance.
     *
     * @param serverConfiguration the XObject holding the remote server configuration
     * @return the configured URL, in the format {@code http://remote.host.name/bin/}, or {@code null} if the
     *         configuration isn't valid
     */
    private String getBaseURL(BaseObject serverConfiguration)
    {
        if (serverConfiguration != null) {
            String result = serverConfiguration.getStringValue("url");
            if (StringUtils.isBlank(result)) {
                return null;
            }

            if (!result.startsWith("http")) {
                result = "http://" + result;
            }
            return StringUtils.stripEnd(result, "/") + "/bin/data/sync";
        }
        return null;
    }

    private String getToken(BaseObject serverConfiguration)
    {
        if (serverConfiguration != null) {
            return serverConfiguration.getStringValue("token");
        }
        return null;
    }

    /**
     * Helper method for obtaining a valid xcontext from the execution context.
     *
     * @return the current request context
     */
    private XWikiContext getXContext()
    {
        return (XWikiContext) this.execution.getContext().getProperty(XWikiContext.EXECUTIONCONTEXT_KEY);
    }
}

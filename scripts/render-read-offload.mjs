#!/usr/bin/env node
import { parseArgs } from "node:util";

// Emits manifests only. It never contacts Kubernetes, copies credentials,
// changes an Ingress or replaces an existing coordinator/PVC.
const { values } = parseArgs({ options: {
  image: { type: "string" },
  namespace: { type: "string", default: "paperclip" },
  name: { type: "string", default: "paperclip-readers" },
  "coordinator-name": { type: "string", default: "paperclip-app" },
  "public-url": { type: "string" },
  secret: { type: "string", default: "paperclip-app-secrets" },
  "instance-id": { type: "string", default: "default" },
  "create-coordinator": { type: "boolean", default: false },
  "storage-class": { type: "string" },
  "storage-size": { type: "string", default: "200Gi" },
  "trust-proxy": { type: "string" },
  "install-kubernetes-cancellation-overlay": { type: "boolean", default: false },
} });
if (!values.image || !/^[\w./:-]+@sha256:[a-f0-9]{64}$/.test(values.image)) throw new Error("--image must name the validated image by immutable sha256 digest");
if (!values["public-url"]) throw new Error("--public-url is required and must match the coordinator's auth origin");
const publicUrl = new URL(values["public-url"]);
if (!["http:", "https:"].includes(publicUrl.protocol) || publicUrl.username || publicUrl.password || publicUrl.pathname !== "/" || publicUrl.search || publicUrl.hash) throw new Error("--public-url must be an HTTP(S) origin");
for (const value of [values.namespace, values.name, values["coordinator-name"], values.secret]) {
  if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(value) || value.length > 63) throw new Error("Kubernetes names must be DNS labels of at most 63 characters");
}
if (values.name === values["coordinator-name"]) throw new Error("Readers and coordinator must have distinct names");
if (values["create-coordinator"] && !values["storage-class"]) throw new Error("New rehearsal coordinators require an explicit block --storage-class (never NFS)");
if (values["install-kubernetes-cancellation-overlay"] && !values["create-coordinator"]) throw new Error("The pinned Kubernetes cancellation overlay requires an explicitly created rehearsal coordinator");
if (!/^[1-9][0-9]*(?:Gi|Ti)$/.test(values["storage-size"])) throw new Error("--storage-size must be a positive Gi or Ti quantity");
const namespace = values.namespace;
const coordinator = values["coordinator-name"];
const env = [
  { name: "PORT", value: "3100" },
  { name: "HOST", value: "0.0.0.0" },
  { name: "PAPERCLIP_HOME", value: "/paperclip" },
  { name: "PAPERCLIP_INSTANCE_ID", value: values["instance-id"] },
  { name: "PAPERCLIP_DEPLOYMENT_MODE", value: "authenticated" },
  { name: "PAPERCLIP_DEPLOYMENT_EXPOSURE", value: "public" },
  { name: "PAPERCLIP_PUBLIC_URL", value: publicUrl.origin },
  { name: "PAPERCLIP_AUTH_BASE_URL_MODE", value: "explicit" },
  { name: "PAPERCLIP_AUTH_PUBLIC_BASE_URL", value: publicUrl.origin },
  { name: "PAPERCLIP_ALLOWED_HOSTNAMES", value: publicUrl.hostname },
  { name: "PAPERCLIP_AUTH_DISABLE_SIGN_UP", value: "true" },
  { name: "SERVE_UI", value: "true" },
  { name: "DATABASE_URL", valueFrom: { secretKeyRef: { name: values.secret, key: "DATABASE_URL" } } },
  { name: "BETTER_AUTH_SECRET", valueFrom: { secretKeyRef: { name: values.secret, key: "BETTER_AUTH_SECRET" } } },
  ...(values["trust-proxy"] ? [{ name: "TRUST_PROXY", value: values["trust-proxy"] }] : []),
];
const readerLabels = { "app.kubernetes.io/name": "paperclip", "app.kubernetes.io/instance": values.name, "paperclip.ai/role": "reader" };
const securityContext = { runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000, seccompProfile: { type: "RuntimeDefault" } };
const containerSecurity = { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } };
const items = [{
  apiVersion: "apps/v1", kind: "Deployment", metadata: { name: values.name, namespace },
  spec: {
    replicas: 2,
    strategy: { type: "RollingUpdate", rollingUpdate: { maxUnavailable: 0, maxSurge: 1 } },
    selector: { matchLabels: readerLabels },
    template: { metadata: { labels: readerLabels }, spec: {
      automountServiceAccountToken: false,
      securityContext,
      containers: [{
        name: "paperclip", image: values.image,
        command: ["/usr/bin/tini", "--", "node", "--import", "./server/node_modules/tsx/dist/loader.mjs", "server/dist/reader.js"],
        env: [...env, { name: "PAPERCLIP_SERVER_ROLE", value: "reader" }, { name: "PAPERCLIP_COORDINATOR_URL", value: `http://${coordinator}:3100` }],
        ports: [{ name: "http", containerPort: 3100 }],
        securityContext: { ...containerSecurity, readOnlyRootFilesystem: true },
        resources: { requests: { cpu: "250m", memory: "512Mi" }, limits: { cpu: "2", memory: "2Gi" } },
        startupProbe: { tcpSocket: { port: "http" }, periodSeconds: 5, failureThreshold: 60 },
        readinessProbe: { httpGet: { path: "/_paperclip/reader-health", port: "http" }, periodSeconds: 5 },
        livenessProbe: { tcpSocket: { port: "http" }, initialDelaySeconds: 30 },
        volumeMounts: [{ name: "scratch", mountPath: "/tmp" }, { name: "home", mountPath: "/paperclip" }],
      }],
      volumes: [{ name: "scratch", emptyDir: {} }, { name: "home", emptyDir: {} }],
    } },
  },
}, {
  apiVersion: "v1", kind: "Service", metadata: { name: values.name, namespace },
  spec: { selector: readerLabels, ports: [{ name: "http", port: 3100, targetPort: "http" }] },
}];
if (values["create-coordinator"]) {
  const labels = { "app.kubernetes.io/name": "paperclip", "app.kubernetes.io/instance": coordinator, "paperclip.ai/role": "coordinator" };
  items.unshift({
    apiVersion: "v1", kind: "PersistentVolumeClaim", metadata: { name: `${coordinator}-data`, namespace },
    spec: { accessModes: ["ReadWriteOnce"], storageClassName: values["storage-class"], resources: { requests: { storage: values["storage-size"] } } },
  }, {
    apiVersion: "v1", kind: "Service", metadata: { name: coordinator, namespace },
    spec: { selector: labels, ports: [{ name: "http", port: 3100, targetPort: "http" }] },
  }, {
    apiVersion: "apps/v1", kind: "StatefulSet", metadata: { name: coordinator, namespace },
    spec: { serviceName: coordinator, replicas: 1, selector: { matchLabels: labels }, template: { metadata: { labels }, spec: {
      securityContext,
      ...(values["install-kubernetes-cancellation-overlay"] ? { initContainers: [{
        name: "kubernetes-cancellation-overlay",
        image: values.image,
        command: ["node", "docker/read-offload/install-kubernetes-overlay.mjs", "/paperclip/adapter-plugins/kubernetes-sandbox"],
        securityContext: { ...containerSecurity, readOnlyRootFilesystem: true },
        volumeMounts: [{ name: "data", mountPath: "/paperclip" }],
      }] } : {}),
      containers: [{
        name: "paperclip", image: values.image,
        env: [...env,
          { name: "PAPERCLIP_SERVER_ROLE", value: "coordinator" },
          { name: "PAPERCLIP_MIGRATION_PROMPT", value: "never" },
          { name: "PAPERCLIP_MIGRATION_AUTO_APPLY", value: "false" },
          { name: "HEARTBEAT_SCHEDULER_ENABLED", value: "false" },
          { name: "PAPERCLIP_DB_BACKUP_ENABLED", value: "false" },
          { name: "PAPERCLIP_SECRETS_MASTER_KEY", valueFrom: { secretKeyRef: { name: values.secret, key: "PAPERCLIP_SECRETS_MASTER_KEY" } } },
        ],
        ports: [{ name: "http", containerPort: 3100 }],
        securityContext: containerSecurity,
        resources: { requests: { cpu: "1", memory: "2Gi" }, limits: { cpu: "4", memory: "8Gi" } },
        readinessProbe: { httpGet: { path: "/api/health", port: "http" }, periodSeconds: 5 },
        volumeMounts: [{ name: "data", mountPath: "/paperclip" }],
      }],
      volumes: [{ name: "data", persistentVolumeClaim: { claimName: `${coordinator}-data` } }],
    } } },
  });
}
process.stdout.write(`${JSON.stringify({ apiVersion: "v1", kind: "List", items }, null, 2)}\n`);

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({ clients: {} as Record<string, unknown> }));
vi.mock("../../src/kube-client.js", () => ({
  createKubeConfig: () => ({}),
  makeKubeClients: () => h.clients,
}));
import plugin from "../../src/plugin.js";

const input = {
  driverKey: "kubernetes",
  companyId: "acme",
  environmentId: "environment",
  providerLeaseId: "pc-owned",
  config: { inCluster: true, backend: "sandbox-cr" },
  leaseMetadata: { namespace: "paperclip-acme", backend: "sandbox-cr", podName: null },
  cancelActiveWork: true,
};
function notFound() {
  return Object.assign(new Error("not found"), { code: 404 });
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("Kubernetes explicit Stop receipts", () => {
  it("waits for both workload and owned pod disappearance, without stopping a neighbouring lease", async () => {
    let workloadGone = false;
    let ownedPodGone = false;
    const deletedWorkloads: string[] = [];
    h.clients = {
      custom: {
        deleteNamespacedCustomObject: async ({ name }: { name: string }) => { deletedWorkloads.push(name); },
        getNamespacedCustomObject: async () => {
          if (workloadGone) throw notFound();
          return { status: { phase: "Terminating" } };
        },
      },
      core: {
        listNamespacedPod: async () => ({ items: [
          { metadata: { name: "pc-owned-neighbour", ownerReferences: [{ kind: "Sandbox", name: "pc-owned-neighbour" }] } },
          ...(!ownedPodGone ? [{ metadata: {
            name: "controller-generated-pod",
            deletionTimestamp: "2026-01-01T00:00:00Z",
            ownerReferences: [{ kind: "Sandbox", name: "pc-owned" }],
          }, status: { phase: "Running" } }] : []),
        ] }),
      },
    };
    let settled = false;
    const stopping = plugin.definition.onEnvironmentReleaseLease!(input).finally(() => { settled = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(settled).toBe(false);
    workloadGone = true;
    await vi.advanceTimersByTimeAsync(1000);
    expect(settled).toBe(false);
    ownedPodGone = true;
    await vi.advanceTimersByTimeAsync(1000);
    expect(await stopping).toEqual({ providerLeaseId: "pc-owned", state: "destroyed" });
    expect(deletedWorkloads).toEqual(["pc-owned"]);
  });

  it("does not manufacture a receipt when Kubernetes cannot confirm absence", async () => {
    const forbidden = Object.assign(new Error("forbidden"), { code: 403 });
    h.clients = {
      custom: {
        deleteNamespacedCustomObject: async () => {},
        getNamespacedCustomObject: async () => { throw forbidden; },
      },
      core: { listNamespacedPod: async () => ({ items: [] }) },
    };
    await expect(plugin.definition.onEnvironmentReleaseLease!(input)).rejects.toBe(forbidden);
  });

  it("fails boundedly while a terminating pod remains", async () => {
    h.clients = {
      custom: {
        deleteNamespacedCustomObject: async () => {},
        getNamespacedCustomObject: async () => { throw notFound(); },
      },
      core: { listNamespacedPod: async () => ({ items: [{ metadata: { name: "pc-owned" } }] }) },
    };
    const outcome = plugin.definition.onEnvironmentReleaseLease!(input).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(46000);
    expect(await outcome).toBeInstanceOf(Error);
  });
});

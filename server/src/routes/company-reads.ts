import { Router } from "express";
import { badRequest } from "../errors.js";
import type { CompanyReadService } from "../services/company-reads.js";
import { assertBoard, assertCompanyAccess, hasCompanyAccess } from "./authz.js";

export function companyReadRoutes(svc: CompanyReadService) {
  const router = Router();
  router.get("/", async (req, res) => {
    assertBoard(req);
    const scope = req.query.scope;
    if (scope !== undefined && scope !== "accessible") {
      throw badRequest("scope must be a single accessible value when provided");
    }
    const result = await svc.list();
    // Navigation needs the same membership scope as company detail routes.
    // Instance admins can inspect the directory without membership, but that
    // visibility alone does not let them open a company's inbox or tasks.
    if (scope === "accessible") {
      res.json(result.filter((company) => hasCompanyAccess(req, company.id)));
      return;
    }
    if (req.actor.source === "local_implicit" || req.actor.isInstanceAdmin) {
      res.json(result);
      return;
    }
    const allowed = new Set(req.actor.companyIds ?? []);
    res.json(result.filter((company) => allowed.has(company.id)));
  });

  router.get("/stats", async (req, res) => {
    assertBoard(req);
    const allowed = req.actor.source === "local_implicit" || req.actor.isInstanceAdmin
      ? null
      : new Set(req.actor.companyIds ?? []);
    const stats = await svc.stats();
    if (!allowed) {
      res.json(stats);
      return;
    }
    const filtered = Object.fromEntries(Object.entries(stats).filter(([companyId]) => allowed.has(companyId)));
    res.json(filtered);
  });

  router.get("/:companyId", async (req, res) => {
    const companyId = req.params.companyId as string;
    assertCompanyAccess(req, companyId);
    // Allow agents (CEO) to read their own company; board always allowed
    if (req.actor.type !== "agent") {
      assertBoard(req);
    }
    const company = await svc.getById(companyId);
    if (!company) {
      res.status(404).json({ error: "Company not found" });
      return;
    }
    res.json(company);
  });
  return router;
}

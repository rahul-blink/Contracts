
## Deploy with Chef (Blinkit Apps — NOT Progress Chef)

Chef here means the internal Blinkit Apps platform at `apps.blinkit.in`.
This is NOT Progress Chef, the configuration-management tool. There are no
cookbooks, recipes, knife, or chef-client. The `chef` CLI is a client for a
gateway that deploys user projects onto an internal Kubernetes cluster.

### Never do this (it will fail or fight the platform)

- **Do NOT use `aws`**, `aws ecr`, `aws eks`, STS, or any AWS CLI command as
  part of a chef deploy. You do not have, and do not need, AWS credentials.
  `chef push` brokers ECR pushes through chef-server.
- **Do NOT use `kubectl`**, `kustomize`, `helm`, or any direct cluster access.
  You do not have, and do not need, a kubeconfig. `chef apply / get / describe / logs / exec / status / quota` are the only cluster verbs.
- **Do NOT use `docker login`**, `docker push` to ECR, or `docker tag`
  targeting ECR. `chef push` and `chef skaffold up` handle image push.
- **Do NOT write Ingress, Namespace, or NetworkPolicy** into your manifests.
  chef-server owns the public route and injects the namespace.
- **Do NOT `cat chef --help` looking for cookbook commands.** There are none.

### Quick start
1. Write `skaffold.yaml` (metadata.name = project name, build.artifacts, deploy.kubectl.manifests)
2. Write k8s manifests under `k8s/`
3. Run `chef skaffold up`

Code changed? Just run `chef skaffold up` again. It rebuilds, pushes a new image, and redeploys the same project.

### Rules
- One ClusterIP Service named exactly `metadata.name` from skaffold.yaml, with at least one port
- No Ingress objects (chef-server owns the public route)
- No Namespace objects (chef injects it)
- Images in manifests use the artifact name from skaffold.yaml (e.g. `image: backend`). Chef rewrites them.

### Postgres on k8s
Set PGDATA to a subdirectory of the mount to avoid the lost+found conflict:
```yaml
env:
  - name: PGDATA
    value: /var/lib/postgresql/data/pgdata
```

### Auth
```
chef auth login    # one-time
chef auth status   # verify
```

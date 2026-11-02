# Using AWS safely

The cloud milestones deploy to AWS. Your CI proves the Terraform is valid and secure without an account, so deploying is
optional until you want it. When you do:

1. **Practise first (optional).** AWS Builder Center offers a free sandbox for selected workshops: 8 hours, one per week,
   nothing persists. Good for trying a service, not for your project.
2. **Use your own new account on the Free plan.** Never an employer account. The plan ends after 6 months or when the
   credits run out, whichever comes first. Stick to services the plan includes: S3, IAM, Lambda, ECS, EventBridge,
   CloudWatch, SNS, Athena, Glue, Secrets Manager, Systems Manager and Budgets. Redshift, EMR, Kinesis and MSK need a paid plan.
   The current list: https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html
3. **Secure it before building.** Turn on MFA for the root user, then create a budget alert of a few dollars that emails you.
4. **Never work as root.** Create an IAM user or role for Terraform with only what the project needs. Keep its access keys in
   your shell environment or `aws configure`. They never go in this repository. Delete the keys when you finish.
5. **One region.** Pick one and keep every resource there.
6. **Plan, then apply.** Run `terraform plan` and read it. Apply, then check the Billing console.
7. **Tear down after each session.** `terraform destroy`. Use `force_destroy = true` on lab buckets so they empty cleanly.

## CI for the cloud milestone

Add this job to `.github/workflows/ci.yml` (it needs no AWS account):

```yaml
  terraform:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
      - run: terraform -chdir=infra init -backend=false
      - run: terraform -chdir=infra validate
      - run: pip install checkov && checkov -d infra --quiet
```

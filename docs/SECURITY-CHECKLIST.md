# Security checklist

Each rule from PRD §5, each cross-user rule, and each security fix from the P2–P4 reviews, with the test that proves it. Test names are `file::Class::method`; run the Python ones with `.venv/bin/python -m unittest <module>.<Class>.<method>`. Tests under `tests/integration/` need a live dev server and keys and aren't part of the unit suite.

Status as of P5 (2026-10-16):
- **59 rules and sub-rules** (§5 plus cross-user isolation) have at least one automated test, and so do the **15 security fixes** from the P2–P4 reviews.
- **2 have no automated test** (listed at the end with the reason). The local JWT-secret refusal was implemented by the Lead in P5 and is now tested.

## §5.1 The approval gate fails closed

| Rule | Test |
|---|---|
| Judge timeout escalates | `tests/test_approvals.py::EvaluatorInWorkflow::test_timeout_escalates` |
| Judge error escalates | `tests/test_approvals.py::EvaluatorInWorkflow::test_exception_escalates` |
| Low confidence escalates (even a deny) | `tests/test_approvals.py::EvaluatorInWorkflow::test_low_confidence_escalates`, `tests/test_approvals.py::Decide::test_low_confidence_escalates_even_a_deny` |
| Malformed verdict escalates | `tests/test_approvals.py::Decide::test_malformed_verdicts_escalate`, `tests/test_approvals.py::EvaluatorInWorkflow::test_malformed_verdict_escalates` |
| Irreversible approve escalates | `tests/test_approvals.py::Decide::test_irreversible_approve_escalates` |
| Deterministic pre-rules (curl, wget, nc, ssh, git push, base64, `.git/hooks`, CI config, env dumps) escalate without a judge call | `tests/test_approvals.py::PreRules::test_positives_escalate_with_the_expected_rule`, `::test_at_least_thirty_cases`, `::test_never_approves_or_denies`, `::test_oversized_input_escalates_unscreened`; `tests/test_approvals.py::EvaluatorInWorkflow::test_pre_rule_escalates_without_judge_call`; `tests/test_p3_operations.py::DemoBehaviourTests::test_exfiltration_through_the_same_pipes_still_escalates` |
| "APPROVED BY REVIEWER" injection escalates; args fenced as data | `tests/test_approvals.py::EvaluatorInWorkflow::test_injection_in_args_escalates_even_if_judge_would_approve`; `tests/test_approvals.py::Prompt::test_args_are_fenced_as_data`, `::test_tool_description_is_excluded`, `::test_bad_nonce_rejected` |
| A judge APPROVE counts only in an allowed criteria set | `tests/test_approvals.py::Decide::test_catalog_approves_only_read_only_and_workspace_edit` |
| No spend account, or cap reached: escalate without a model call | `tests/test_approvals.py::EvaluatorInWorkflow::test_no_spend_account_escalates_without_judge_call`, `::test_spend_blocked_verdict_escalates` |

## §5.2 Risk classes

| Rule | Test |
|---|---|
| Tools map to their classes; unknown tools are `external_side_effect` | `tests/test_approvals.py::CriteriaAndPostures::test_criteria_sets_and_thresholds`, `::test_tool_assignment`; `tests/test_agent_routes.py::ProductViewConfigTests::test_tool_risk_classes_come_from_the_agent_rulebook` |
| `read_only` is safe | `tests/test_callbacks.py::CheckTests::test_tools_are_safe_callbacks`; `tests/test_code_mode.py::TypeCheckTests::test_risk_classes` |
| "Always allow" only for `workspace_edit`; never for shell or commit | `tests/test_approvals.py::CriteriaAndPostures::test_remember_allowed_only_for_workspace_edit`; `tests/test_tools_workspace.py::ToolDeclarationTests::test_git_commit_is_shell_class` |
| UI shows "Always approve" only where it applies, never for MCP (P5) | `simple_chat_agent/web/src/session/progress.test.ts` ("risk classes"); `simple_chat_agent/web/src/decisions/render.test.tsx` ("renders one commit as the plain card with no Always approve, and a reason") |
| `fetch_url` and `search_web` are `network` | `tests/test_tools_research.py::ToolDefinitionTests::test_network_risk_class_in_source` |
| MCP and artifacts are `external_side_effect` | `tests/test_mcp.py::ToolsetTests::test_default_risk_class_is_external_side_effect`; `tests/test_tools_artifacts.py::ToolDefinitionTests::test_risk_classes` |
| In the catalog the judge approves only `read_only` and `workspace_edit`; shell and commit go to a human | `tests/test_approvals.py::CriteriaAndPostures::test_judge_approvable_classes`, `::test_git_commit_needs_a_human_in_catalog`; `tests/test_approvals.py::EvaluatorInWorkflow::test_catalog_shell_goes_to_human_without_judge_call`, `::test_catalog_git_commit_goes_to_human_without_judge_call` |
| The catalog refuses a caller's policy that pre-allows risky tools | `tests/test_approvals.py::CriteriaAndPostures::test_catalog_refuses_preallowing_risky_tools` |
| "Never ask" is refused in the catalog | `tests/test_approvals.py::CriteriaAndPostures::test_never_ask_refused_in_catalog`; `tests/test_agent_routes.py::ProductViewConfigTests::test_create_refuses_postures_the_menu_greys_out`, `::test_set_posture_message_refuses_the_same` |

## §5.3 The model can't change its own guardrails

| Rule | Test |
|---|---|
| `set_model`, `set_posture`, `set_web_search`, `forget_always_allowed` are `model_callable=False` | `tests/test_security_rules.py::GuardrailHandlersTests::test_settings_handlers_are_not_model_callable` (P5) |
| Subagents don't get them | `tests/test_agent_routes.py::AgentInterfaceTests::test_subagent_interfaces_by_workflow_type`; `tests/test_p3_operations.py::ForgetAlwaysAllowedTests::test_subagents_do_not_have_it` |
| Criteria edits | No handler exists, so there is nothing to call (see the end) |

## §5.4 SSRF guard

| Rule | Test |
|---|---|
| Private, loopback, link-local, CGNAT and metadata addresses are refused | `tests/test_net_guard.py::ClassifyAddressTests::test_every_blocked_range_is_refused`, `::test_metadata_endpoint_is_named` |
| Resolve, then refuse (including mixed answers and odd IPv4 spellings) | `tests/test_net_guard.py::CheckUrlTests::test_hostname_resolving_to_private_is_refused`, `::test_mixed_public_and_private_answers_are_refused`, `::test_alternate_ipv4_spellings_are_resolved_then_refused` |
| Every redirect is re-checked | `tests/test_net_guard.py::GuardedGetRedirectTests::test_redirect_from_public_url_to_loopback_is_refused`, `::test_each_hop_is_resolved_and_pinned_separately`, `::test_cross_origin_redirect_drops_credentials`; `tests/test_net_guard.py::PinnedConnectionTests::test_real_redirect_to_internal_targets_is_refused` |
| The IP is pinned (no DNS rebinding) | `tests/test_net_guard.py::PinnedConnectionTests::test_connects_to_the_vetted_ip_and_keeps_the_host_header`, `::test_dns_rebinding_on_a_later_hop_is_caught`; `tests/test_net_guard.py::PinnedTransportTests::test_refuses_any_other_host_or_port` |
| `fetch_url` uses it | `tests/test_tools_research.py::FetchUrlRetryTests::test_guard_refusal_is_non_retryable_tool_failure`, `::test_redirect_to_metadata_is_refused` |
| MCP server URLs use it at register, call and every request | `tests/test_mcp.py::SsrfTests::test_loopback_and_metadata_refused_by_default`, `::test_guarded_transport_checks_every_request`; `tests/test_mcp.py::CallActivityTests::test_ssrf_refused`; `tests/test_mcp.py::McpRouteTests::test_local_ssrf_refused_at_register` |
| Local MCP loopback only on the named ports; public hosts stay on the base ports (P5) | `tests/test_mcp.py::SsrfTests::test_loopback_allowlist_is_local_only`, `::test_loopback_exception_is_only_on_the_named_ports` |
| The browser opens only an https (or local http) OAuth URL (P5) | `simple_chat_agent/web/src/mcp/authUrl.test.ts` ("safeAuthUrl") |

## §5.5 Executor scoping

| Rule | Test |
|---|---|
| `workspace_id` comes from the session, never from the model | `tests/test_agent_routes.py::SessionTests::test_create_our_body`; `tests/test_tools_workspace.py::ToolDeclarationTests::test_workspace_id_is_hidden_from_the_model`; `tests/test_code_mode.py::WorkspaceFSTests::test_only_the_calling_sessions_workspace` |
| A subagent id resolves to its root session's owner | `tests/test_agent_routes.py::OwnershipTests::test_subagent_id_resolves_to_root_owner`, `::test_describe_fallback_rejects_untagged_or_foreign_types` |
| `/files/*` confines paths (no `..`, absolute paths, symlinks at any level, hard links, FIFOs, sockets) | `tests/test_executor_files.py::ExecutorFilesConfinementTests` (10 tests, e.g. `::test_mid_path_symlink_to_sibling_workspace_is_refused`); `tests/test_executor_files.py::ExecutorFilesOperationTests::test_workspaces_are_isolated_by_id` |
| 1 MB read and 5 MB write caps | `tests/test_executor_files.py::ExecutorFilesOperationTests::test_sparse_file_over_the_cap_is_too_large_but_a_range_read_works`, `::test_write_cap_and_parent_creation` |
| Every executor route needs the bearer token; none configured means 503 | `tests/test_executor_files.py::ExecutorFilesAuthTests::test_every_endpoint_requires_the_bearer_token`, `::test_unconfigured_token_refuses_requests` |
| Auth is checked before the body is parsed (P5) | `tests/test_executor_files.py::ExecutorFilesAuthTests::test_auth_is_checked_before_the_body_is_parsed` |
| The API checks ownership before calling the executor | `tests/test_tools_artifacts.py::ImportAttachmentTests::test_ownership_is_checked_before_writing`; `tests/test_callbacks.py::CallbackRouteTests::test_upload_limits_and_owner`; `tests/test_console.py::OwnershipTests::test_files_view_is_404_even_for_the_owner` |
| `pick_files` uploads only while that pick waits (P5) | `tests/test_callbacks.py::CallbackRouteTests::test_upload_only_while_that_pick_waits`; `tests/test_agent_bridge.py::ForwarderTests::test_open_pick_files_calls_are_mirrored` |

## §5.6 Secrets

| Rule | Test |
|---|---|
| No keys in the executor (its self-test fails if it sees any) | `tests/test_executor_files.py::ExecutorCredentialBoundaryTests::test_provider_keys_fail_the_self_test`, `::test_mcp_oauth_and_github_token_names_fail_the_self_test`, `::test_clean_environment_passes_the_env_check` |
| MCP tokens resolve only for the session owner | `tests/test_mcp.py::OwnerTokenTests::test_owner_gets_bearer_header`, `::test_cross_user_auth_ref_refused`; `tests/test_mcp.py::CallActivityTests::test_cross_user_auth_ref_refused` |
| A failed bearer re-add keeps the working token (P5) | `tests/test_mcp.py::McpRouteTests::test_failed_bearer_readd_keeps_the_working_token` |
| Abandoned MCP OAuth flows time out and are capped per user (P5) | `tests/test_mcp.py::OAuthFlowTimeoutTests::test_abandoned_flow_times_out`; `tests/test_mcp.py::McpRouteTests::test_oauth_flows_are_capped_per_user` |
| Judge status reads the worker env without returning keys | `tests/test_p3_operations.py::SecurityReviewTests::test_judge_status_reads_worker_env_without_returning_keys` |
| `**/.env*` is in `.gitignore` and `.dockerignore` | `tests/test_security_rules.py::EnvFilesIgnoredTests::test_gitignore_covers_env_files`, `::test_dockerignore_covers_env_files` (P5) |
| Local auth refuses to start without a JWT secret (stricter than the PRD: no built-in fallback anywhere, 32+ characters) | `tests/test_session_secret.py` (`test_no_built_in_fallback`, `test_short_secret_refused_and_long_one_works`, `test_api_refuses_to_start_with_local_auth_and_no_secret`) |

## §5.7 Budgets

| Rule | Test |
|---|---|
| Max output tokens clamped per model | `tests/test_context.py::BudgetHelperTests::test_budget_for_clamps_output_reservation`, `::test_model_limits_cover_the_allowlist` |
| Context fitting (A17): a 200-turn session stays under 85% of the window | `tests/test_context.py::LongSessionTests::test_200_turn_session_never_exceeds_85_percent`; `tests/test_context.py::FitHistoryPropertyTests::test_randomized_histories_keep_invariants_and_are_idempotent` |
| History budget (A20): checkpoint at 60% of the caps | `tests/test_budget.py::HistoryBudgetTests` (7 tests) |
| Spend caps (A21) block, and fail closed when Redis is down | `tests/test_budget.py::SpendGuardRedisTests::test_session_cap`, `::test_user_day_cap_and_day_rollover`, `::test_global_breaker_blocks_everyone`; `tests/test_budget.py::SpendGuardFailClosedTests::test_check_fails_closed_when_redis_is_down`, `::test_from_env_fails_closed_on_dead_url` |
| Subagent depth 1: children get read-only tools and no subagent tools | `tests/test_security_rules.py::SubagentDepthTests::test_children_get_read_only_tools_only` (P5) |

## §5.8 Retry policies by class

| Rule | Test |
|---|---|
| Model: 4 attempts, 2 s, ×2, 60 s cap | `tests/test_security_rules.py::ModelRetryPolicyTests::test_model_activity_policy` (P5) |
| Model: a 4xx other than 408/409/429 is non-retryable | `tests/test_security_rules.py::ModelRetryPolicyTests::test_provider_4xx_is_final_and_408_409_429_5xx_retry` (P5) |
| Read tools: 5 attempts | `tests/test_tools_research.py::ToolDefinitionTests::test_retry_policies_are_read_class`; `tests/test_tools_artifacts.py::ToolDefinitionTests::test_retry_policies_are_explicit` |
| `run_command` runs at most once after it started; reattaches on retry | `tests/test_tools_workspace.py::RunCommandActivityTests::test_retry_after_worker_crash_reattaches_and_runs_once`, `::test_unreachable_executor_is_retryable`; `tests/test_tools_workspace.py::ExecutorJobTests::test_lost_job_after_executor_restart_is_not_rerun`, `::test_cached_result_with_a_different_command_is_rejected` |
| Write and commit are idempotent per tool-call id | `tests/test_tools_workspace.py::FileToolsTests::test_edit_retry_after_lost_response_reports_already_applied`; `tests/test_tools_workspace.py::ExecToolsTests::test_git_status_diff_commit`; `tests/test_tools_artifacts.py::CreateArtifactTests::test_same_idempotency_key_returns_the_same_artifact` |
| Judge: 2 attempts, 20 s schedule-to-close | `tests/test_approvals.py::EvaluatorWiring::test_judge_model_config` |
| A file tool's failure reaches the model as its reason (P5) | `tests/test_harness_ext.py::ToolFailureText::test_activity_failure_reaches_the_model_as_its_reason` |

## Cross-user isolation (S1, S5)

| Rule | Test |
|---|---|
| User B gets 404 on every route for A's session and subagents | `tests/test_agent_routes.py::OwnershipTests::test_foreign_root_session_is_404_everywhere`, `::test_unknown_id_is_404_everywhere`, `::test_malformed_id_is_404_without_temporal`, `::test_subagent_fallback_with_wiped_redis` |
| Session lists are per user | `tests/test_agent_routes.py::SessionTests::test_list_is_per_user_newest_first` |
| Console routes are owner-scoped and need auth | `tests/test_console.py::OwnershipTests::test_foreign_session_and_subagents_are_404`, `::test_unknown_id_is_404`; `tests/test_console.py::SessionListTests::test_list_holds_only_the_callers_sessions`; `tests/test_console.py::AuthTests::test_every_api_route_requires_auth` |
| MCP servers, attachments and artifacts are owner-scoped | `tests/test_mcp.py::McpRouteTests::test_local_add_list_remove_owner_scoped`; `tests/test_tools_artifacts.py::ReadAttachmentTests::test_other_owner_session_or_generated_artifact_is_not_found` |
| The catalog never accepts the local session cookie | `tests/test_catalog_integration.py::CatalogIntegrationTests::test_catalog_auth_does_not_accept_an_app_session_cookie` |

## Review fixes

### P2 deep review (security-relevant)

| Fix | Test |
|---|---|
| Minor 2: messages to a subagent are refused; subagent approvals use the child's Update | `tests/test_agent_routes.py::ReviewFixTests::test_minor2_messages_to_a_subagent_are_refused`, `::test_minor2_subagent_approvals_use_the_childs_update` |
| Minor 5: cross-site requests refused (CSRF); JSON routes need a JSON content type | `tests/test_agent_routes.py::ReviewFixTests::test_minor5_cross_site_requests_are_refused`, `::test_minor5_json_routes_need_a_json_content_type`; `tests/test_console.py::ReviewFixTests::test_cross_site_posts_to_console_routes_are_refused` |
| Minor 6 and m6: upload size bounded, including chunked bodies | `tests/test_agent_routes.py::ReviewFixTests::test_minor6_upload_size_is_bounded`; `tests/test_agent_routes.py::VerificationFollowUpTests::test_m6_chunked_upload_is_bounded` |
| Minor 7: the owner claim is set-if-absent | `tests/test_agent_routes.py::ReviewFixTests::test_minor7_owner_claim_is_nx`; `tests/test_agent_bridge.py::StoreAdditionsTests::test_claim_owner_is_set_nx` |
| Minor 11: Describe misses are cached briefly (no probing by id) | `tests/test_agent_routes.py::ReviewFixTests::test_minor11_describe_misses_are_cached_briefly` |
| Minor 12: internal errors aren't echoed | `tests/test_agent_routes.py::ReviewFixTests::test_minor12_internal_errors_are_not_echoed` |
| N9: `Origin: null` never falls back to Referer | `tests/test_agent_routes.py::VerificationFollowUpTests::test_n9_origin_null_never_falls_back_to_referer` |

The P2 reliability fixes (B1, M1–M5, minors 1, 4, 8–10, 13, 15, N1–N7) are tested too; the test names carry the fix id (`test_b1_…`, `test_m1_…`, `test_n5_…`) in `tests/test_agent_routes.py`, `tests/test_agent_bridge.py` and `tests/test_agent_telemetry.py`.

### P3 security review

| Fix | Test |
|---|---|
| #2: a subagent replay marker for an unstored turn is kept | `tests/test_agent_bridge.py::SubagentMarkerTests::test_review_2_marker_for_an_unstored_later_turn_is_kept` |
| #3: a NUL byte or bad path is 404, not 500 | `tests/test_agent_routes.py::ProductViewMountTests::test_nul_and_bad_paths_are_404_not_500` |
| #4: app pages carry a CSP | `tests/test_agent_routes.py::ProductViewMountTests::test_app_pages_carry_the_csp` |
| #5: an attachment must be this user's upload to this session; fields checked and names cleaned | `tests/test_agent_routes.py::SecurityReviewTests::test_attachment_must_be_this_users_upload_to_this_session`, `::test_attachment_fields_are_checked_and_names_cleaned`; `tests/test_p3_operations.py::AttachmentTests::test_review_5_injection_fields_stay_on_one_data_line` |
| #6: `request_id` shape and reserved prefix | `tests/test_agent_routes.py::SecurityReviewTests::test_request_id_shape_and_reserved_prefix` |
| #7: the session list follows the agent's applied setting, not the request | `tests/test_agent_bridge.py::SettingsResultTests::test_settings_results_reach_on_settings_and_others_do_not`; `tests/test_agent_routes.py::DirectorySyncTests::test_model_and_posture_changes_update_the_list`, `::test_a_change_the_worker_refused_puts_the_old_value_back` |
| #9: test tools local only; auto mode refused when the worker has no judge | `tests/test_p3_operations.py::SecurityReviewTests::test_test_tools_only_in_local`, `::test_auto_is_refused_when_the_worker_has_no_judge`; `tests/test_agent_routes.py::ProductViewConfigTests::test_test_tools_local_only` |

### P4 review

| Fix | Test |
|---|---|
| `pick_files` caps reserved atomically across tool ids; a failed write gives its slot back | `tests/test_callbacks.py::CallbackRouteTests::test_upload_session_caps_span_tool_ids`, `::test_upload_limits_and_owner` |

## Rules with no automated test

| Rule | Why | Where it stands |
|---|---|---|
| §5.5 Each workspace activity resolves its owning `CodingAgent` by walking `parent_id` (at most 3 levels, cached) | Built differently. Activities never take a workspace from the model: `workspace_id` is injected from init data (`agent/workflow.py`, `agent/subagents.py`), so there is no walk to test in the worker. The API's walk from a subagent id to its owner allows up to 4 hops (`agent_routes.py:_owner_from_temporal`, `MAX_PARENT_HOPS`) and is tested above. | The PRD wording should be updated to match. |
| §5.3 Criteria edits are `model_callable=False` | There is no criteria-edit handler. Criteria come from init and the catalog sanitizes caller policies (`test_catalog_refuses_preallowing_risky_tools`). | Nothing to test until a handler exists. |

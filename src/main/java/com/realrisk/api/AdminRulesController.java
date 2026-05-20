package com.realrisk.api;

import com.realrisk.model.AdminRuleRequest;
import com.realrisk.model.RuleView;
import com.realrisk.rules.RuleService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/rules")
public class AdminRulesController {
  private final RuleService ruleService;

  public AdminRulesController(RuleService ruleService) {
    this.ruleService = ruleService;
  }

  @PostMapping
  public ResponseEntity<RuleView> upsertRule(@Valid @RequestBody AdminRuleRequest request) {
    return ResponseEntity.status(HttpStatus.CREATED).body(ruleService.upsertRule(request));
  }

  @GetMapping
  public ResponseEntity<List<RuleView>> listRules() {
    return ResponseEntity.ok(ruleService.listRules());
  }

  @DeleteMapping("/{ruleId}")
  public ResponseEntity<Void> disableRule(@PathVariable String ruleId) {
    ruleService.disableRule(ruleId);
    return ResponseEntity.noContent().build();
  }
}

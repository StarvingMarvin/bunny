package org.rabix.bindings.cwl.expression.javascript;

import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang3.StringUtils;
import org.rabix.bindings.cwl.bean.CWLRuntime;
import org.rabix.bindings.cwl.expression.CWLExpressionException;
import org.rabix.common.helper.JSONHelper;

import com.fasterxml.jackson.databind.JsonNode;

import jdk.nashorn.api.scripting.NashornScriptEngineFactory;

public class CWLExpressionJavascriptResolver {

  public final static int TIMEOUT_IN_SECONDS = 5;

  public final static String EXPR_CONTEXT_NAME = "inputs";
  public final static String EXPR_SELF_NAME = "self";
  public final static String EXPR_RUNTIME_NAME = "runtime";

  /**
   * Evaluate JS script (function or statement)
   */
  public static Object evaluate(Object context, Object self, String expr, CWLRuntime runtime, List<String> engineConfigs) throws CWLExpressionException {
    String trimmedExpr = StringUtils.trim(expr);
    if (trimmedExpr.startsWith("$")) {
      trimmedExpr = trimmedExpr.substring(1);
    }

    ScriptEngineManager engineManager = new ScriptEngineManager();
    NashornScriptEngineFactory factory = (NashornScriptEngineFactory) engineManager.getEngineFactories().get(0);
    ScriptEngine engine = factory.getScriptEngine("--language=es6");
    try {
      if (engineConfigs != null) {
        for (int i = 0; i < engineConfigs.size(); i++) {
          engine.eval(engineConfigs.get(i));
        }
      }
      engine.put(EXPR_CONTEXT_NAME, context);
      engine.put(EXPR_SELF_NAME, self);
      engine.put(EXPR_RUNTIME_NAME, runtime);

      Object result = resolve(trimmedExpr, engine);
      if (result == null) {
        return null;
      }
      return castResult(result);
    } catch (Exception e) {
      throw new CWLExpressionException(e.getMessage() + " encountered while resolving expression: " + expr, e);
    }
  }

  private static Object resolve(String trimmedExpr, ScriptEngine engine) throws ScriptException {
    String f = "function f()";
    if (trimmedExpr.startsWith("{")) {
      f += trimmedExpr;
    } else {
      f = f + "{return " + trimmedExpr + "}";
    }
    engine.eval(f);
    return engine.eval("JSON.stringify(f());");
  }

  /**
   * Cast result to proper Java object
   */
  private static Object castResult(Object result) {
    JsonNode node = JSONHelper.readJsonNode(result.toString());
    return JSONHelper.transform(node, false);
  }
}

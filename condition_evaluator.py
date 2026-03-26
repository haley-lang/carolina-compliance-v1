# Condition Evaluator Utility

class ConditionEvaluator:
    @staticmethod
    def evaluate(condition, payload):
        condition_type = condition.get("type")
        key = condition.get("key")
        value = condition.get("value")

        if condition_type == "equals":
            return ConditionEvaluator.equals(payload, key, value)
        elif condition_type == "less_than_days":
            return ConditionEvaluator.less_than_days(payload, key, value)
        elif condition_type == "exists":
            return ConditionEvaluator.exists(payload, key)
        return False

    @staticmethod
    def equals(payload, key, value):
        return payload.get(key) == value

    @staticmethod
    def less_than_days(payload, key, value):
        # Placeholder for less_than_days logic
        return False

    @staticmethod
    def exists(payload, key):
        return key in payload
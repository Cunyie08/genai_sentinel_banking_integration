# This file contains the central logging for Sentinnel bank to responsibly write structured reasoning logs (audit/evaluation purposes)


import json
from datetime import datetime
from typing import Dict, Any

# Create a class that handles strutured logging of agents decisions
class ReasoningLogger:

    Log_File = "app/logs/reasoning.log"

    @classmethod #
    # Write a reasoning log entry
    def log(cls, agent_name:str, payload: Dict[str, Any]) -> None:

        """ 
        Args: agent_name: Agent producing the log
        
        Returns: Decision data to be logged
        """
        log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "agent": agent_name,
        "payload": payload
        }

        # Add the log entry as a JSON
        with open(cls.Log_File, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")
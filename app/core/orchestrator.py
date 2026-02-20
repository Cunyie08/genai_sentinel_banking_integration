# This file contains the System's Brain (Orchestrator) for Sentinnel Bank

"""
This coordinates the agent execution using AgentGraph
"""

from app.agents.dispatcher_agent import DispatcherAgent
from app.agents.sentinel_agent import SentinelAgent
#from app.agents.trajectory_agent import TrajectoryAgent
from app.core.graph import AgentGraph
from app.utils.logger import ReasoningLogger


# Central coordinator class for agent execution
class Orchestrator: 
    
    # Initialize the agents and routing graph
    def __init__(self):
      
      # Initialize the agents
      self.dispatcher = DispatcherAgent()
      self.sentinel = SentinelAgent(name="SentinelAgent")
      #self.trajectory = TrajectoryAgent()

      # Initialize the routing graphs
      self.graph = AgentGraph()

      # Create an agent registry for easy search
      self.agent_registry = {
         "dispatcher_agent": self.dispatcher,
         "sentinel_agent": self.sentinel,
         #"trajectory_agent": self.trajectory
      }

    # Handle incoming customer complaint
    def handle_request(self,complaint_text: str) -> dict: # Raw complaint text -> final system response
   
        # Dispatcher interprets the complaint
        dispatch_result = self.dispatcher.run({"complaint_text": complaint_text})

        # To determine the next agent via graph
        next_agent_name = self.graph.get_next_agent(dispatch_result)

        # Fetch the agent object
        next_agent = self.agent_registry.get(next_agent_name)

        # Log routing decision
        ReasoningLogger.log(
            agent_name="Orchestrator",
            payload={
                "from_agent": dispatch_result.get("agent"),
                "to_agent": next_agent_name,
                "department": dispatch_result.get("department")
            }
        )

        # To handle failure
        if next_agent is None:
            return {
                "error": "No valid agent found",
                "dispatch_result": dispatch_result
            }

        # Execute the selected agent
        final_agent = next_agent.run(dispatch_result)

        return final_agent


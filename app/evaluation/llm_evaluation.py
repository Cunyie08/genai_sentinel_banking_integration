# This file contains LLM acting as a Judge to evaluate the result (Who deiced if the AI did well)

"""
This contains an evaluation interface with the llm acting as a judge 

Implementation to be added in week 3
"""

class judge: # Compares an agent's output with the ground-truth

    # Takes two arguments, the agent's decision and the expected outcome

    def evaluate(self, prediction: dict, expected: dict) -> dict: # Evaluation result is a dictionary
        
        raise NotImplementedError() # The judgement logic will be implemented in week 3
    

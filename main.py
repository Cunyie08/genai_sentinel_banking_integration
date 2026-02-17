from app.core.orchestrator import Orchestrator


# Testing
if __name__ == "__main__":
    orchestrator = Orchestrator()

    complaint = "Someone used my card at 10am"

    result = orchestrator.handle_request(complaint)

    print(result)
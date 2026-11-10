# Self-Healing Microservice Mesh

### A Go-Based Control Plane with Reconciliation-Driven Fault Recovery

## Project Overview

Self-Healing Microservice Mesh is a Go-based project designed to demonstrate automated failure detection and recovery in a microservice environment.

The system uses a central control plane to monitor multiple microservices, detect unhealthy instances, restart failed services, and route traffic only to healthy service replicas.

## Problem Statement

Modern applications are divided into multiple independent microservices. Any service can crash, hang, or become slow, resulting in downtime.

Manually identifying and fixing these failures takes time. This project aims to build a system that can detect and recover from service failures automatically.

## Key Features

- Automated health monitoring of microservices.
- Failure detection through health probes.
- Automatic restart of failed service instances.
- Removal of unhealthy instances from traffic routing.
- Automatic recovery and reintegration of healthy instances.
- Reconciliation-driven control loop.
- Live monitoring dashboard.

## System Architecture

The system consists of the following components:

- **Client:** Sends requests to the application.
- **Reverse Proxy / Load Balancer:** Routes traffic to healthy service instances.
- **Microservices:** Service A, Service B, and Service C.
- **Chaos Injector:** Simulates service failures.
- **Control Plane:** Monitors services and manages recovery.

### Control Plane Components

- **Prober:** Checks the health of service instances.
- **Reconciler:** Compares desired and actual system states.
- **Supervisor:** Restarts failed services and manages recovery.

## Reconciliation Loop

The control plane follows four main stages:

1. **Observe:** Collect health probe results from service replicas.
2. **Compare:** Compare the desired state with the actual replica state.
3. **Act:** Restart failed services and update routing.
4. **Wait:** Sleep for one tick and repeat the process.

The reconciliation loop is level-triggered, meaning it acts on the current system state rather than relying only on individual events.

## Technologies and Go Concepts

- Go (Golang)
- Goroutines
- Channels
- Context and timeouts
- Mutex / Atomic
- HTTP
- Concurrent Programming

## Failure Recovery Workflow

1. A service instance crashes.
2. Health probes begin failing.
3. After three failed probes, the instance is marked unhealthy.
4. The unhealthy instance is removed from routing.
5. The supervisor restarts the service.
6. After successful health checks, the instance rejoins traffic rotation.

## How to Run

### Prerequisites

- Go installed on your system.

### Run the Project

```bash
git clone <your-repository-url>
cd self-healing-microservice-mesh
go run ./cmd/controlplane
```

Open the dashboard:

```text
http://localhost:8080
```

Keep the terminal running while using the dashboard.

## Project Documentation

The project presentation is available here:

[View Project Presentation (PDF)](Self_Healing_Mesh_ppt.pdf)

## Team Members

| Name | Student ID |
|---|---|
| Tripti | 2301010278 |
| Rizpah | 2301010219 |
| Raghuveer | 2301010268 |
| Ayush | 2301010253 |
| Aman | 2301010277 |
| Sarthak | 2301010216 |

## Future Scope

- Leader election.
- Automatic scaling.
- Further dashboard enhancements.

---

**Project:** Self-Healing Microservice Mesh  
**Language:** Go  
**Evaluation:** First Evaluation

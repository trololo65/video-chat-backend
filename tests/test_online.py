from fastapi.testclient import TestClient

from app.main import app, random_match_manager


def _reset_match_state() -> None:
    random_match_manager.connections.clear()
    random_match_manager.waiting_queue.clear()
    random_match_manager.partner_by_client.clear()
    random_match_manager.mode_by_client.clear()


def test_online_is_zero_without_clients():
    _reset_match_state()
    with TestClient(app) as client:
        response = client.get("/online")
        assert response.status_code == 200
        assert response.json() == {"online": 0}

        aliased = client.get("/api/online")
        assert aliased.status_code == 200
        assert aliased.json() == {"online": 0}


def test_online_counts_connected_random_match_clients():
    _reset_match_state()
    with TestClient(app) as client:
        with client.websocket_connect("/ws/random-match") as first:
            welcome = first.receive_json()
            assert welcome["type"] == "welcome"

            response = client.get("/online")
            assert response.status_code == 200
            assert response.json() == {"online": 1}

            with client.websocket_connect("/ws/random-match") as second:
                second.receive_json()
                assert client.get("/online").json() == {"online": 2}

            assert client.get("/online").json() == {"online": 1}

        assert client.get("/online").json() == {"online": 0}


def test_online_stays_while_client_is_searching():
    _reset_match_state()
    with TestClient(app) as client:
        with client.websocket_connect("/ws/random-match") as ws:
            ws.receive_json()
            ws.send_json({"type": "ready", "mode": "video"})
            searching = ws.receive_json()
            assert searching["type"] == "searching"
            assert client.get("/online").json() == {"online": 1}

import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend,
} from 'chart.js';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import './App.css';

// Register ChartJS components
ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend
);

function App() {
    const [sensorData, setSensorData] = useState([]);
    const [kpis, setKpis] = useState({ total_yield: 1250.5, avg_moisture: 45.2, active_sensors: 24 });

    useEffect(() => {
        // Connect to WebSocket
        const ws = new WebSocket(`ws://${window.location.hostname}:8000/ws/live`);

        ws.onopen = () => {
            console.log('Connected to WebSocket');
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                setSensorData(prev => {
                    const newData = [...prev, data];
                    return newData.slice(-20); // Keep last 20 readings
                });
            } catch (e) {
                console.error('Error parsing message', e);
            }
        };

        return () => ws.close();
    }, []);

    // Prepare chart data
    const chartData = {
        labels: sensorData.map((d, i) => d.timestamp ? d.timestamp.split(' ')[1] : i),
        datasets: [
            {
                label: 'Air Temp (°C)',
                data: sensorData.map(d => d.air_temperature_c),
                borderColor: 'rgb(255, 99, 132)',
                tension: 0.1,
            },
            {
                label: 'Soil Temp (°C)',
                data: sensorData.map(d => d.soil_temperature_c),
                borderColor: 'rgb(75, 192, 192)',
                tension: 0.1,
            },
        ],
    };

    // Chatbot state
    const [chatOpen, setChatOpen] = useState(false);
    const [messages, setMessages] = useState([
        { text: "Hello! I'm your Farm Assistant. Ask me about status, temperature, or moisture.", sender: "bot" }
    ]);
    const [input, setInput] = useState("");

    const sendMessage = async () => {
        if (!input.trim()) return;

        const userMsg = { text: input, sender: "user" };
        setMessages(prev => [...prev, userMsg]);
        setInput("");

        try {
            const response = await fetch(`http://${window.location.hostname}:8000/api/chat`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ message: userMsg.text })
            });
            const data = await response.json();
            setMessages(prev => [...prev, { text: data.response, sender: "bot" }]);
        } catch (error) {
            console.error("Chat error:", error);
            setMessages(prev => [...prev, { text: "Sorry, I couldn't reach the server.", sender: "bot" }]);
        }
    };

    return (
        <div className="dashboard">
            <header className="header">
                <h1>🌱 Farm IoT Dashboard</h1>
                <div className="status-badge">System Online</div>
            </header>

            <div className="kpi-grid">
                <div className="kpi-card">
                    <h3>Total Yield</h3>
                    <p>{kpis.total_yield} kg</p>
                </div>
                <div className="kpi-card">
                    <h3>Avg Moisture</h3>
                    <p>{kpis.avg_moisture}%</p>
                </div>
                <div className="kpi-card">
                    <h3>Active Sensors</h3>
                    <p>{kpis.active_sensors}</p>
                </div>
            </div>

            <div className="main-content">
                <div className="chart-container">
                    <h2>Real-time Data</h2>
                    <div style={{ height: '300px' }}>
                        <Line data={chartData} options={{ maintainAspectRatio: false }} />
                    </div>
                </div>

                <div className="map-container">
                    <h2>Farm Location</h2>
                    <div style={{ height: '300px' }}>
                        <MapContainer center={[30.05, 31.25]} zoom={13} style={{ height: '100%', width: '100%' }}>
                            <TileLayer
                                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                                attribution='&copy; OpenStreetMap contributors'
                            />
                            <Marker position={[30.05, 31.25]}>
                                <Popup>Greenhouse A</Popup>
                            </Marker>
                        </MapContainer>
                    </div>
                </div>
            </div>

            {/* Chatbot UI */}
            <div className={`chatbot-container ${chatOpen ? 'open' : ''}`}>
                <div className="chatbot-header" onClick={() => setChatOpen(!chatOpen)}>
                    <span>🤖 Farm Assistant</span>
                    <span>{chatOpen ? '▼' : '▲'}</span>
                </div>
                {chatOpen && (
                    <div className="chatbot-body">
                        <div className="messages">
                            {messages.map((msg, i) => (
                                <div key={i} className={`message ${msg.sender}`}>
                                    {msg.text}
                                </div>
                            ))}
                        </div>
                        <div className="input-area">
                            <input
                                type="text"
                                value={input}
                                onChange={(e) => setInput(e.target.value)}
                                onKeyPress={(e) => e.key === 'Enter' && sendMessage()}
                                placeholder="Ask me something..."
                            />
                            <button onClick={sendMessage}>Send</button>
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

export default App;

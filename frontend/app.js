// Configuration
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;
const WS_URL = import.meta.env.VITE_WS_URL;

// State
let ws = null;
let reconnectInterval = null;
let alertsChart = null;
let lastAlertCount = 0;
let currentChartView = 'hourly'; // Track current view
let lastTotalAlerts = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initializeWebSocket();
    loadInitialData();
    initializeChart();
    setupChartButtons();
    setInterval(updateMetrics, 5000);
});

// Setup chart view buttons
function setupChartButtons() {
    const buttons = document.querySelectorAll('.chart-btn');
    
    buttons.forEach(btn => {
        btn.addEventListener('click', function() {
            // Remove active class from all buttons
            buttons.forEach(b => b.classList.remove('active'));
            
            // Add active class to clicked button
            this.classList.add('active');
            
            // Update chart view
            const view = this.textContent.toLowerCase();
            currentChartView = view;
            updateChart(view);
        });
    });
}

// WebSocket Connection
function initializeWebSocket() {
    updateConnectionStatus('connecting');
    
    ws = new WebSocket(WS_URL);
    
    ws.onopen = () => {
        console.log('WebSocket connected');
        updateConnectionStatus('connected');
        clearInterval(reconnectInterval);
    };
    
    ws.onmessage = (event) => {
        const message = JSON.parse(event.data);
        handleRealtimeUpdate(message);
    };
    
    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        updateConnectionStatus('disconnected');
    };
    
    ws.onclose = () => {
        console.log('WebSocket disconnected');
        updateConnectionStatus('disconnected');
        
        reconnectInterval = setInterval(() => {
            console.log('Attempting to reconnect...');
            initializeWebSocket();
        }, 3000);
    };
}

function updateConnectionStatus(status) {
    const badge = document.getElementById('connection-badge');
    const dot = document.getElementById('status-dot');
    const text = document.getElementById('status-text');
    
    if (status === 'connected') {
        dot.classList.add('connected');
        text.textContent = 'Connected';
        badge.style.borderColor = 'var(--success)';
    } else if (status === 'connecting') {
        dot.classList.remove('connected');
        text.textContent = 'Connecting...';
        badge.style.borderColor = 'var(--warning)';
    } else {
        dot.classList.remove('connected');
        text.textContent = 'Disconnected';
        badge.style.borderColor = 'var(--danger)';
    }
}

// Handle real-time updates
function handleRealtimeUpdate(message) {
    if (message.type === 'transaction') {
        addTransaction(message.data);
    } else if (message.type === 'alert') {
        addAlert(message.data);
        showNotification('Fraud Alert!', `$${message.data.transaction.amount} flagged`);
    }
}

// Load initial data
async function loadInitialData() {
    try {
        const [txnResponse, alertResponse] = await Promise.all([
            fetch(`${API_BASE_URL}/transactions?limit=10`),
            fetch(`${API_BASE_URL}/alerts?limit=10`)
        ]);
        
        const txnData = await txnResponse.json();
        const alertData = await alertResponse.json();
        
        txnData.transactions.forEach(txn => addTransaction(txn));
        alertData.alerts.forEach(alert => addAlert(alert));
        
        await updateMetrics();
        await updateChart(currentChartView);
        
    } catch (error) {
        console.error('Error loading initial data:', error);
    }
}

// Update metrics
async function updateMetrics() {
    try {
        const response = await fetch(`${API_BASE_URL}/metrics`);
        const data = await response.json();
        
        animateValue('total-transactions', data.total_transactions);
        if (lastTotalAlerts === null || data.total_alerts !== lastTotalAlerts) {
        animateValue('total-alerts', data.total_alerts);
        lastTotalAlerts = data.total_alerts;
   }

        
        document.getElementById('fraud-rate').textContent = data.fraud_rate.toFixed(2) + '%';
        document.getElementById('amount-flagged').textContent = formatCurrency(data.total_amount_flagged);
        
        // Update alert badge
        document.getElementById('alert-count-badge').textContent = `${data.total_alerts} Active`;
        
        // Update alert change indicator
        const change = data.total_alerts - lastAlertCount;
        if (change > 0) {
            document.getElementById('alert-change').textContent = `↑ ${change} new`;
            document.getElementById('alert-change').className = 'stat-change negative';
        }
        lastAlertCount = data.total_alerts;
        
        // Update rate trend
        const rateTrend = document.getElementById('rate-trend');
        if (data.fraud_rate > 15) {
            rateTrend.textContent = '↑ High';
            rateTrend.className = 'stat-change negative';
        } else if (data.fraud_rate > 5) {
            rateTrend.textContent = '→ Moderate';
            rateTrend.className = 'stat-change neutral';
        } else {
            rateTrend.textContent = '↓ Low';
            rateTrend.className = 'stat-change positive';
        }
        
    } catch (error) {
        console.error('Error updating metrics:', error);
    }
}

// Animate number changes
function animateValue(elementId, end) {
    const element = document.getElementById(elementId);
    const start = parseInt(element.textContent) || 0;
    const duration = 1000;
    const range = end - start;
    const increment = range / (duration / 16);
    let current = start;
    
    const timer = setInterval(() => {
        current += increment;
        if ((increment > 0 && current >= end) || (increment < 0 && current <= end)) {
            current = end;
            clearInterval(timer);
        }
        element.textContent = Math.floor(current).toLocaleString();
    }, 16);
}

// Add transaction to UI
function addTransaction(transaction) {
    const container = document.getElementById('transactions-container');
    
    const emptyState = container.querySelector('.empty-state');
    if (emptyState) {
        emptyState.remove();
    }
    
    const txnElement = document.createElement('div');
    txnElement.className = 'transaction-item';
    
    const amount = parseFloat(transaction.amount || 0);
    const timestamp = new Date(transaction.timestamp);
    const time = timestamp.toLocaleTimeString();
    
    txnElement.innerHTML = `
        <div class="tx-col">
            <div class="tx-id">${transaction.transaction_id}</div>
        </div>
        <div class="tx-col">
            <div class="tx-merchant">${transaction.merchant || 'Unknown'}</div>
        </div>
        <div class="tx-col" style="text-transform: capitalize;">
            ${transaction.merchant_category || 'N/A'}
        </div>
        <div class="tx-col">
            <div class="tx-amount">${formatCurrency(amount)}</div>
        </div>
        <div class="tx-col">
            <span class="tx-status legitimate">✓ Legitimate</span>
        </div>
        <div class="tx-col">
            <div class="tx-time">${time}</div>
        </div>
    `;
    
    container.insertBefore(txnElement, container.firstChild);
    
    while (container.children.length > 20) {
        container.removeChild(container.lastChild);
    }
}

// Add alert to UI
function addAlert(alert) {
    const container = document.getElementById('alerts-container');
    
    const emptyState = container.querySelector('.empty-state');
    if (emptyState) {
        emptyState.remove();
    }
    
    const alertElement = document.createElement('div');
    alertElement.className = 'alert-item';
    
    const transaction = alert.transaction || {};
    const amount = parseFloat(transaction.amount || 0);
    const fraudScore = (alert.fraud_score * 100).toFixed(1);
    const timestamp = new Date(alert.timestamp);
    const time = timestamp.toLocaleTimeString();
    
    alertElement.innerHTML = `
        <div class="alert-header">
            <div class="alert-id">🚨 ${transaction.transaction_id || 'Unknown'}</div>
            <div class="alert-amount">${formatCurrency(amount)}</div>
        </div>
        <div class="alert-details">
            <strong>${transaction.merchant || 'Unknown Merchant'}</strong><br>
            📍 ${transaction.location || 'Unknown'} • ${transaction.merchant_category || 'Unknown'}<br>
            ⏰ ${time} • User: ${transaction.user_id || 'Unknown'}
        </div>
        <div class="alert-score">
            ⚠️ Fraud Confidence: <strong>${fraudScore}%</strong>
        </div>
        <div class="alert-analysis">
            <strong>AI Analysis:</strong><br>
            ${alert.analysis || 'Suspicious pattern detected by machine learning model.'}
        </div>
    `;
    
    container.insertBefore(alertElement, container.firstChild);
    
    while (container.children.length > 10) {
        container.removeChild(container.lastChild);
    }
    
    // Update chart when new alert is added
    updateChart(currentChartView);
}

// Initialize Chart
function initializeChart() {
    const ctx = document.getElementById('alertsChart').getContext('2d');
    
    alertsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Fraud Alerts',
                data: [],
                borderColor: '#ef4444',
                backgroundColor: 'rgba(239, 68, 68, 0.1)',
                tension: 0.4,
                fill: true,
                borderWidth: 2,
                pointRadius: 4,
                pointHoverRadius: 6,
                pointBackgroundColor: '#ef4444',
                pointBorderColor: '#1e293b',
                pointBorderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: '#1e293b',
                    titleColor: '#e2e8f0',
                    bodyColor: '#e2e8f0',
                    borderColor: '#334155',
                    borderWidth: 1,
                    padding: 12,
                    displayColors: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: 1,
                        color: '#94a3b8'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    }
                },
                x: {
                    ticks: {
                        color: '#94a3b8'
                    },
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    }
                }
            }
        }
    });
}

// Update chart based on view (hourly or daily)
async function updateChart(view = 'hourly') {
    try {
        const endpoint = view === 'daily' ? '/analytics/daily' : '/analytics/hourly';
        const response = await fetch(`${API_BASE_URL}${endpoint}`);
        const data = await response.json();
        
        let labels = [];
        let counts = [];
        
        if (view === 'daily' && data.daily_alerts) {
            labels = data.daily_alerts.map(item => {
                const date = new Date(item.day);
                return date.toLocaleDateString([], { month: 'short', day: 'numeric' });
            });
            counts = data.daily_alerts.map(item => item.count);
        } else if (view === 'hourly' && data.hourly_alerts) {
            labels = data.hourly_alerts.map(item => {
                const date = new Date(item.hour);
                return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            });
            counts = data.hourly_alerts.map(item => item.count);
        }
        
        // Update chart
        alertsChart.data.labels = labels.slice(-12);
        alertsChart.data.datasets[0].data = counts.slice(-12);
        alertsChart.data.datasets[0].label = view === 'daily' ? 'Daily Fraud Alerts' : 'Hourly Fraud Alerts';
        alertsChart.update('active');
        
    } catch (error) {
        console.error('Error updating chart:', error);
    }
}

// Format currency
function formatCurrency(amount) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 2
    }).format(amount);
}

// Show notification
function showNotification(title, message) {
    console.log(`${title}: ${message}`);
    
    // Browser notification (optional)
    if ("Notification" in window && Notification.permission === "granted") {
        new Notification(title, {
            body: message,
            icon: '🚨'
        });
    }
}

// Request notification permission on load (optional)
if ("Notification" in window && Notification.permission === "default") {
    Notification.requestPermission();
}

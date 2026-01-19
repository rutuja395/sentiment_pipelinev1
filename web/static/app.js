let currentLocation = '';

// Load locations on page load
document.addEventListener('DOMContentLoaded', () => {
    loadLocations();
    setupEventListeners();
});

function setupEventListeners() {
    document.querySelectorAll('.mode-btn').forEach(btn => {
        btn.addEventListener('click', () => switchMode(btn.dataset.mode));
    });
    
    document.getElementById('locationSelect').addEventListener('change', (e) => {
        currentLocation = e.target.value;
        loadStats();
        loadInsights();
    });
    
    document.getElementById('regenerateBtn').addEventListener('click', () => loadInsights(true));
    document.getElementById('chatSend').addEventListener('click', sendChat);
    document.getElementById('chatInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') sendChat();
    });
    document.getElementById('applyFilters').addEventListener('click', applyFilters);
}

function switchMode(mode) {
    document.querySelectorAll('.mode-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.mode-content').forEach(content => content.classList.remove('active'));
    
    document.querySelector(`[data-mode="${mode}"]`).classList.add('active');
    document.getElementById(`${mode}Mode`).classList.add('active');
}

async function loadLocations() {
    const res = await fetch('/api/locations');
    const data = await res.json();
    const select = document.getElementById('locationSelect');
    select.innerHTML = data.locations.map(loc => `<option value="${loc}">${loc}</option>`).join('');
    currentLocation = data.locations[0];
    loadStats();
    loadInsights();
}

async function loadStats() {
    if (!currentLocation) return;
    const res = await fetch(`/api/stats/${currentLocation}`);
    const data = await res.json();
    document.getElementById('totalReviews').textContent = data.total_reviews || 0;
    document.getElementById('avgRating').textContent = data.average_rating || 0;
}

async function loadInsights(regenerate = false) {
    if (!currentLocation) return;
    const res = await fetch(`/api/insights/${currentLocation}?regenerate=${regenerate}`);
    const data = await res.json();
    
    let html = '<h3>Top Topics</h3><ul>';
    (data.top_topics || []).forEach(t => {
        html += `<li>${t.topic}: ${t.count} mentions</li>`;
    });
    html += '</ul>';
    
    html += '<h3>Key Drivers</h3>';
    html += '<h4>Complaints:</h4><ul>';
    (data.key_drivers?.complaints || []).forEach(d => {
        html += `<li>${d.topic}: ${d.count}</li>`;
    });
    html += '</ul>';
    
    document.getElementById('insightsContent').innerHTML = html;
}

async function sendChat() {
    const input = document.getElementById('chatInput');
    const query = input.value.trim();
    if (!query) return;
    
    addMessage(query, 'user');
    input.value = '';
    
    const res = await fetch('/api/chat', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({query, location_id: currentLocation})
    });
    const data = await res.json();
    addMessage(data.answer, 'assistant');
}

function addMessage(text, type) {
    const div = document.createElement('div');
    div.className = `message ${type}`;
    div.textContent = text;
    document.getElementById('chatHistory').appendChild(div);
}

async function applyFilters() {
    const rating = document.getElementById('ratingFilter').value;
    const sentiment = document.getElementById('sentimentFilter').value;
    
    let url = `/api/reviews?location_id=${currentLocation}`;
    if (rating) url += `&min_rating=${rating}&max_rating=${rating}`;
    if (sentiment) url += `&sentiment=${sentiment}`;
    
    const res = await fetch(url);
    const data = await res.json();
    
    let html = `<h3>Results: ${data.count} reviews</h3>`;
    data.reviews.slice(0, 10).forEach(r => {
        html += `<div class="review-card">
            <strong>Rating: ${r.rating}</strong><br>
            ${r.review_text.substring(0, 200)}...
        </div>`;
    });
    
    document.getElementById('reviewsResults').innerHTML = html;
}

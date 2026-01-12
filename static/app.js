// DemirAI v2 - Chat Interface
document.addEventListener('DOMContentLoaded', () => {
  const qInput = document.getElementById('q');
  const sendBtn = document.getElementById('send');
  const chatSection = document.getElementById('chat');
  const resultSection = document.getElementById('result');
  const toggleSqlBtn = document.getElementById('toggleSql');
  const sqlBox = document.getElementById('sqlBox');

  if (!qInput || !sendBtn || !chatSection || !resultSection || !toggleSqlBtn || !sqlBox) {
    console.error('Gerekli elementler bulunamadı!', {
      qInput: !!qInput,
      sendBtn: !!sendBtn,
      chatSection: !!chatSection,
      resultSection: !!resultSection,
      toggleSqlBtn: !!toggleSqlBtn,
      sqlBox: !!sqlBox
    });
    return;
  }

  let sqlVisible = false;

  // Quick question buttons
  document.querySelectorAll('.quick').forEach(btn => {
    btn.addEventListener('click', () => {
      const question = btn.getAttribute('data-q');
      qInput.value = question;
      ask(question);
    });
  });

  // Send button
  sendBtn.addEventListener('click', () => {
    const question = qInput.value.trim();
    if (question.length < 3) return;
    ask(question);
  });

  // Enter key
  qInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      sendBtn.click();
    }
  });

  // Toggle SQL
  toggleSqlBtn.addEventListener('click', () => {
    sqlVisible = !sqlVisible;
    sqlBox.classList.toggle('hidden', !sqlVisible);
  });

  async function ask(question) {
    // Clear previous result
    resultSection.classList.add('hidden');
    chatSection.innerHTML = '';
    
    // Add user message
    addMessage('user', question);
    qInput.value = '';
    qInput.disabled = true;
    sendBtn.disabled = true;
    sendBtn.textContent = 'Yükleniyor...';

    try {
      const res = await fetch('/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }

      const data = await res.json();
      
      // Add AI response
      addMessage('ai', data.tldr || 'Sorgu tamamlandı.');
      
      // Show results
      displayResults(data);
      
    } catch (err) {
      addMessage('error', `Hata: ${err.message}`);
    } finally {
      qInput.disabled = false;
      sendBtn.disabled = false;
      sendBtn.textContent = 'Sor';
    }
  }

  function addMessage(role, text) {
    const div = document.createElement('div');
    div.className = `p-4 rounded-2xl ${
      role === 'user' 
        ? 'bg-zinc-800 ml-auto max-w-2xl' 
        : role === 'error'
        ? 'bg-red-900/30 border border-red-800 max-w-2xl'
        : 'bg-zinc-900 max-w-3xl'
    }`;
    div.textContent = text;
    chatSection.appendChild(div);
    chatSection.scrollTop = chatSection.scrollHeight;
  }

  function displayResults(data) {
    resultSection.classList.remove('hidden');
    
    // TLDR
    document.getElementById('tldr').textContent = data.tldr || '';
    
    // Findings
    const findingsEl = document.getElementById('findings');
    findingsEl.innerHTML = '';
    (data.findings || []).forEach(f => {
      const li = document.createElement('li');
      li.textContent = `• ${f}`;
      findingsEl.appendChild(li);
    });
    
    // Recommendations
    const recsEl = document.getElementById('recs');
    recsEl.innerHTML = '';
    (data.recommendations || []).forEach(r => {
      const li = document.createElement('li');
      li.textContent = `• ${r}`;
      recsEl.appendChild(li);
    });
    
    // Picks (if available)
    const picksEl = document.getElementById('picks');
    if (picksEl) {
      picksEl.innerHTML = '';
      // Note: picks field may not be in current response model
    }
    
    // SQL
    document.getElementById('sql').textContent = data.sql || '';
    
    // Scroll to results
    resultSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
});

const { Client, LocalAuth, MessageMedia } = require("whatsapp-web.js");
const qrcode = require("qrcode-terminal");
const fs = require("fs");
const path = require("path");
const { google } = require("googleapis");
const crypto = require("crypto");
const natural = require("natural");
const { WordTokenizer, PorterStemmer } = natural;
const express = require("express");
require("dotenv").config();

// ==================
// EXPRESS SERVER FOR HEALTH CHECKS
// ==================
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// Health check endpoint (required for Railway)
app.get("/", (req, res) => {
  res.json({
    status: "healthy",
    service: "MPB WhatsApp Bot",
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

app.get("/health", (req, res) => {
  try {
    const health = loadHealthStatus();
    res.json(health);
  } catch (error) {
    res.status(500).json({ status: "error", message: error.message });
  }
});

app.get("/stats", (req, res) => {
  try {
    const stats = analytics.getStats();
    res.json(stats);
  } catch (error) {
    res.status(500).json({ status: "error", message: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`🌐 Health server running on port ${PORT}`);
});

// ==================
// CONFIGURATION & VALIDATION
// ==================
const REQUIRED_ENV_VARS = [
  'SHEET_ID',
  'FACTORY_SHEET_ID', 
  'STAFF_SHEET_ID',
  'ELEVATION_AVG_SHEET_ID',
  'DRIVE_FOLDER_ID',
  'GOOGLE_CLIENT_EMAIL',
  'GOOGLE_PRIVATE_KEY'
];

console.log("🔍 Validating environment variables...");
const missingVars = REQUIRED_ENV_VARS.filter(varName => !process.env[varName]);
if (missingVars.length > 0) {
  console.error(`❌ Missing required environment variables: ${missingVars.join(', ')}`);
  console.error(`📝 Please add these to your .env file`);
  process.exit(1);
}
console.log("✅ All required environment variables present\n");

// ==================
// FILE PATHS & DIRECTORIES
// ==================
const DATA_DIR = process.env.DATA_DIR || "./data";
const LOGS_DIR = process.env.LOGS_DIR || "./logs";
const FORWARDED_MESSAGES_FILE = path.join(DATA_DIR, "forwardedMessages.json");
const USER_INTERACTIONS_FILE = path.join(DATA_DIR, "userInteractions.json");
const MESSAGE_CACHE_FILE = path.join(DATA_DIR, "messageCache.json");
const ANALYTICS_FILE = path.join(DATA_DIR, "analytics.json");
const REQUEST_LOG_FILE = path.join(LOGS_DIR, "requests.log");
const ERROR_LOG_FILE = path.join(LOGS_DIR, "errors.log");
const HEALTH_FILE = path.join(DATA_DIR, "health.json");

// Create directories
[DATA_DIR, LOGS_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    console.log(`📁 Created directory: ${dir}`);
  }
});

// ==================
// CONSTANTS & MAPPINGS
// ==================
const ELEVATION_MAP = {
  'uh': 'UH', 'u.h': 'UH', 'upper high': 'UH',
  'wh': 'WH', 'w.h': 'WH', 'western high': 'WH',
  'high': 'H', 'h ': 'H',
  'medium': 'M', 'm ': 'M', 'med': 'M',
  'low': 'L', 'l ': 'L',
  'bt': 'BT', 'b.t': 'BT', 'bottom': 'BT'
};

const ELEVATION_EMOJIS = {
  'UH': '⛰️', 'WH': '🏔️', 'H': '🗻',
  'UM': '🏞️', 'WM': '🌄', 'M': '🌄',
  'L': '🌳', 'BT': '🍃'
};

const DEPARTMENT_MAP = {
  'valuation': 'Valuation',
  'appraisal': 'Valuation',
  'account': 'Accounts',
  'accounts': 'Accounts',
  'tax': 'Accounts',
  'invoice': 'Accounts',
  'vat': 'Accounts',
  'technical': 'IT',
  'it': 'IT',
  'support': 'IT',
  'system': 'IT',
  'marketing': 'Marketing',
  'inquiry': 'Marketing',
  'general': 'Marketing'
};

// Cache settings
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes
const MESSAGE_CACHE_DURATION = 60 * 60 * 1000; // 1 hour
const FORWARDED_MSG_CLEANUP = 24 * 60 * 60 * 1000; // 24 hours
const MAX_ANALYTICS_ENTRIES = 10000; // Prevent memory overflow
const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB

// ==================
// UTILITY: SAFE FILE OPERATIONS
// ==================
function safeReadJSON(filePath, defaultValue = {}) {
  try {
    if (fs.existsSync(filePath)) {
      const data = fs.readFileSync(filePath, 'utf8');
      return JSON.parse(data);
    }
  } catch (error) {
    console.error(`⚠️ Failed to read ${filePath}:`, error.message);
    logError(error, `safeReadJSON: ${filePath}`);
  }
  return defaultValue;
}

function safeWriteJSON(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
    return true;
  } catch (error) {
    console.error(`❌ Failed to write ${filePath}:`, error.message);
    logError(error, `safeWriteJSON: ${filePath}`);
    return false;
  }
}

function rotateLogFile(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      const stats = fs.statSync(filePath);
      if (stats.size > MAX_LOG_SIZE) {
        const backupPath = `${filePath}.${Date.now()}.old`;
        fs.renameSync(filePath, backupPath);
        console.log(`🔄 Rotated log file: ${filePath}`);
      }
    }
  } catch (error) {
    console.error(`⚠️ Log rotation failed:`, error.message);
  }
}

// ==================
// ENHANCED ANALYTICS SYSTEM
// ==================
class EnhancedAnalytics {
  constructor() {
    this.analytics = this.loadAnalytics();
    this.startTime = Date.now();
  }
  
  loadAnalytics() {
    const data = safeReadJSON(ANALYTICS_FILE, {
      total_messages: 0,
      unique_users: [],
      intents: {},
      response_times: [],
      errors: 0,
      successful_requests: 0,
      failed_requests: 0,
      start_time: Date.now()
    });
    
    // Convert array back to Set
    data.unique_users = new Set(data.unique_users || []);
    return data;
  }
  
  saveAnalytics() {
    const saveData = {
      ...this.analytics,
      unique_users: Array.from(this.analytics.unique_users)
    };
    
    // Trim response_times if too large
    if (saveData.response_times.length > MAX_ANALYTICS_ENTRIES) {
      saveData.response_times = saveData.response_times.slice(-1000);
    }
    
    safeWriteJSON(ANALYTICS_FILE, saveData);
  }
  
  trackMessage(userId, intent, responseTime, success = true) {
    this.analytics.total_messages++;
    this.analytics.unique_users.add(userId);
    
    if (!this.analytics.intents[intent]) {
      this.analytics.intents[intent] = 0;
    }
    this.analytics.intents[intent]++;
    
    this.analytics.response_times.push({
      timestamp: Date.now(),
      response_time: responseTime,
      intent: intent,
      success: success
    });
    
    // Keep only recent entries
    if (this.analytics.response_times.length > 1000) {
      this.analytics.response_times = this.analytics.response_times.slice(-1000);
    }
    
    if (success) {
      this.analytics.successful_requests++;
    } else {
      this.analytics.errors++;
      this.analytics.failed_requests++;
    }
    
    // Save periodically (every 10 messages)
    if (this.analytics.total_messages % 10 === 0) {
      this.saveAnalytics();
    }
  }
  
  getStats() {
    const avgTime = this.analytics.response_times.length > 0 
      ? (this.analytics.response_times.reduce((acc, curr) => acc + curr.response_time, 0) / this.analytics.response_times.length).toFixed(2)
      : 0;
    
    const uptime = Math.floor((Date.now() - this.startTime) / 1000);
    const uptimeHours = Math.floor(uptime / 3600);
    const uptimeMinutes = Math.floor((uptime % 3600) / 60);
    
    return {
      total_messages: this.analytics.total_messages,
      unique_users: this.analytics.unique_users.size,
      successful_requests: this.analytics.successful_requests,
      failed_requests: this.analytics.failed_requests,
      popular_intents: Object.entries(this.analytics.intents)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 5),
      average_response_time: avgTime + 'ms',
      error_rate: this.analytics.total_messages > 0 
        ? ((this.analytics.errors / this.analytics.total_messages) * 100).toFixed(2) + '%' 
        : '0%',
      uptime: `${uptimeHours}h ${uptimeMinutes}m`,
      memory_usage: {
        rss: `${(process.memoryUsage().rss / 1024 / 1024).toFixed(2)} MB`,
        heap_used: `${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)} MB`
      }
    };
  }
}

const analytics = new EnhancedAnalytics();

// ==================
// SMART RATE LIMITER (Persistent)
// ==================
class SmartRateLimiter {
  constructor(maxRequests = 15, windowMs = 60000) {
    this.maxRequests = maxRequests;
    this.windowMs = windowMs;
    this.userLimits = new Map();
    this.violations = new Map();
  }

  checkLimit(userId) {
    const now = Date.now();
    let userData = this.userLimits.get(userId);
    
    // Initialize or reset if window expired
    if (!userData || now > userData.resetTime) {
      userData = { 
        count: 0, 
        resetTime: now + this.windowMs 
      };
    }
    
    if (userData.count >= this.maxRequests) {
      // Track violations
      const violations = this.violations.get(userId) || 0;
      this.violations.set(userId, violations + 1);
      return false;
    }
    
    userData.count++;
    this.userLimits.set(userId, userData);
    return true;
  }
  
  getRemainingTime(userId) {
    const userData = this.userLimits.get(userId);
    if (!userData) return 0;
    
    const remaining = Math.ceil((userData.resetTime - Date.now()) / 1000);
    return remaining > 0 ? remaining : 0;
  }
  
  getRemainingRequests(userId) {
    const userData = this.userLimits.get(userId);
    if (!userData) return this.maxRequests;
    
    return Math.max(0, this.maxRequests - userData.count);
  }
  
  cleanup() {
    const now = Date.now();
    let cleaned = 0;
    
    for (const [userId, userData] of this.userLimits.entries()) {
      if (now > userData.resetTime + this.windowMs) {
        this.userLimits.delete(userId);
        cleaned++;
      }
    }
    
    // Clear old violations
    if (this.violations.size > 100) {
      this.violations.clear();
    }
    
    if (cleaned > 0) {
      console.log(`🧹 Cleaned ${cleaned} rate limit entries`);
    }
  }
  
  getViolations(userId) {
    return this.violations.get(userId) || 0;
  }
}

const rateLimiter = new SmartRateLimiter(15, 60000);

// ==================
// ADVANCED NLP CLASSIFIER
// ==================
class AdvancedIntentClassifier {
  constructor() {
    this.tokenizer = new WordTokenizer();
    this.stemmer = PorterStemmer;
    this.patterns = this.initializePatterns();
  }
  
  initializePatterns() {
    return {
      factory_query: {
        keywords: ['mf', 'factory', 'performance', 'average', 'data', 'details', 'code'],
        patterns: [
          /mf\s*[a-z]?\s*\d{3,4}/gi,
          /factory\s+(code|performance|data|average)/gi,
          /\b(mf\d{3,4})\b/gi,
          /\baverage\b.*\bmf\b/gi
        ],
        weight: 10
      },
      elevation_query: {
        keywords: ['elevation', 'uh', 'wh', 'high', 'medium', 'low', 'price'],
        patterns: [
          /elevation\s+(average|price|rate)/gi,
          /(uh|wh|high|medium|low)\s+(average|price)/gi,
          /sale\s+\d+\s+elevation/gi
        ],
        weight: 8
      },
      market_report: {
        keywords: ['market', 'report', 'pdf', 'sale', 'document'],
        patterns: [
          /market\s+report/gi,
          /sale\s+\d+\s+report/gi,
          /pdf\s+report/gi,
          /report\s+sale/gi
        ],
        weight: 9
      },
      department_contact: {
        keywords: ['valuation', 'account', 'technical', 'support', 'it', 'marketing', 'inquiry', 'contact', 'help'],
        patterns: [
          /valuation\s+(department|inquiry|report)/gi,
          /account(s|ing)\s+(department|query|issue)/gi,
          /(technical|it)\s+support/gi,
          /marketing\s+(inquiry|department)/gi
        ],
        weight: 7
      },
      bot_control: {
        keywords: ['stop', 'mute', 'pause', 'disable', 'enable', 'activate', 'unmute', 'resume', 'bot'],
        patterns: [
          /stop\s+(bot|responding|messages)/gi,
          /mute\s+bot/gi,
          /disable\s+bot/gi,
          /pause\s+bot/gi,
          /unmute\s+bot/gi,
          /enable\s+bot/gi,
          /activate\s+bot/gi,
          /resume\s+bot/gi,
          /^(stop|mute|pause)$/gi,
          /^(start|unmute|resume|activate)$/gi
        ],
        weight: 10
      },
      help: {
        keywords: ['help', 'menu', 'start', 'options', 'commands'],
        patterns: [/^help$/i, /^menu$/i, /^start$/i, /what can you do/i],
        weight: 6
      },
      contact: {
        keywords: ['contact', 'email', 'phone', 'address', 'location'],
        patterns: [/contact\s+info/i, /email\s+address/i, /phone\s+number/i],
        weight: 6
      },
      status: {
        keywords: ['status', 'stats', 'statistics', 'analytics'],
        patterns: [/^status$/i, /^stats$/i, /bot\s+status/i],
        weight: 5
      },
      casual_conversation: {
        keywords: ['hi', 'hello', 'hey', 'good morning', 'good evening', 'thanks', 'thank you', 'ok', 'okay', 'bye', 'goodbye'],
        patterns: [
          /^(hi|hello|hey)$/i,
          /^good\s+(morning|afternoon|evening|night)$/i,
          /^(thanks|thank you|thx)$/i,
          /^(ok|okay|k)$/i,
          /^(bye|goodbye|see you)$/i
        ],
        weight: 3
      },
      irrelevant: {
        keywords: ['vacancy', 'vacancies', 'job', 'hiring', 'career', 'recruitment', 'apply'],
        patterns: [
          /any\s+(vacancy|vacancies|job)/gi,
          /(hiring|recruitment|career)/gi,
          /looking\s+for\s+job/gi
        ],
        weight: 8
      }
    };
  }
  
  classify(text) {
    if (!text || typeof text !== 'string') return 'general';
    
    const cleanText = text.toLowerCase().trim();
    const tokens = this.tokenizer.tokenize(cleanText);
    const stems = tokens.map(token => this.stemmer.stem(token));
    
    let bestMatch = 'general';
    let highestScore = 0;
    
    for (const [intent, data] of Object.entries(this.patterns)) {
      let score = 0;
      
      // Keyword matching with stemming
      data.keywords.forEach(keyword => {
        const stemmedKeyword = this.stemmer.stem(keyword);
        if (cleanText.includes(keyword)) score += 2;
        if (stems.includes(stemmedKeyword)) score += 1;
      });
      
      // Pattern matching
      data.patterns.forEach(pattern => {
        const matches = cleanText.match(pattern);
        if (matches) score += matches.length * 3;
      });
      
      // Apply intent weight
      score *= (data.weight || 1);
      
      if (score > highestScore) {
        highestScore = score;
        bestMatch = intent;
      }
    }
    
    return highestScore >= 3 ? bestMatch : 'general';
  }
  
  extractEntities(text) {
    return {
      factory_codes: this.extractFactoryCodes(text),
      sale_number: this.extractSaleNumber(text),
      elevation: this.extractElevation(text),
      department: this.extractDepartment(text)
    };
  }
  
  extractFactoryCodes(text) {
    const matches = text.match(/\b(MF[A-Z]?\s*\d{3,4})\b/gi);
    if (!matches) return [];
    const codes = matches.map(code => code.replace(/\s+/g, '').toUpperCase());
    return [...new Set(codes)].slice(0, 5);
  }
  
  extractSaleNumber(text) {
    const matches = text.match(/sale\s*(?:no\.?|number)?\s*(\d{1,3})/gi);
    if (!matches) return null;
    
    const lastMatch = matches[matches.length - 1];
    const numberMatch = lastMatch.match(/\d+/);
    if (numberMatch) {
      let num = parseInt(numberMatch[0]);
      if (num <= 999) {
        return num.toString().padStart(3, "0");
      }
    }
    return null;
  }
  
  extractElevation(text) {
    const cleanText = text.toLowerCase();
    for (const [key, value] of Object.entries(ELEVATION_MAP)) {
      if (cleanText.includes(key)) {
        return value;
      }
    }
    return null;
  }
  
  extractDepartment(text) {
    const cleanText = text.toLowerCase();
    for (const [key, value] of Object.entries(DEPARTMENT_MAP)) {
      if (cleanText.includes(key)) {
        return value;
      }
    }
    return null;
  }
}

const classifier = new AdvancedIntentClassifier();

// ==================
// MESSAGE DEDUPLICATION
// ==================
class MessageCache {
  constructor() {
    this.cache = safeReadJSON(MESSAGE_CACHE_FILE, {});
  }
  
  saveCache() {
    safeWriteJSON(MESSAGE_CACHE_FILE, this.cache);
  }
  
  isDuplicate(userId, messageHash) {
    const cacheKey = `${userId}_${messageHash}`;
    const now = Date.now();
    
    if (this.cache[cacheKey]) {
      return true;
    }
    
    this.cache[cacheKey] = now;
    
    // Save every 20 entries
    if (Object.keys(this.cache).length % 20 === 0) {
      this.saveCache();
    }
    
    return false;
  }
  
  cleanup() {
    const now = Date.now();
    let cleaned = 0;
    
    for (const [key, timestamp] of Object.entries(this.cache)) {
      if (now - timestamp > MESSAGE_CACHE_DURATION) {
        delete this.cache[key];
        cleaned++;
      }
    }
    
    if (cleaned > 0) {
      this.saveCache();
      console.log(`🧹 Cleaned ${cleaned} message cache entries`);
    }
  }
}

const messageCache = new MessageCache();

// ==================
// USER INTERACTION TRACKING
// ==================
class UserTracker {
  constructor() {
    this.interactions = safeReadJSON(USER_INTERACTIONS_FILE, {});
  }
  
  saveInteractions() {
    safeWriteJSON(USER_INTERACTIONS_FILE, this.interactions);
  }
  
  recordInteraction(userId) {
    if (!this.interactions[userId]) {
      this.interactions[userId] = {
        firstSeen: Date.now(),
        lastSeen: Date.now(),
        messageCount: 0,
        lastWelcome: null,
        botActive: true,
        lastBotResponse: null,
        ignoredMessages: 0
      };
    }
    
    this.interactions[userId].lastSeen = Date.now();
    this.interactions[userId].messageCount++;
    
    // Save every 5 interactions
    if (this.interactions[userId].messageCount % 5 === 0) {
      this.saveInteractions();
    }
  }
  
  shouldSendWelcome(userId) {
    if (!this.interactions[userId]) {
      return true;
    }
    
    const lastWelcome = this.interactions[userId].lastWelcome;
    const now = Date.now();
    const ONE_DAY = 24 * 60 * 60 * 1000;
    
    if (!lastWelcome || (now - lastWelcome) > ONE_DAY) {
      this.interactions[userId].lastWelcome = now;
      this.saveInteractions();
      return true;
    }
    
    return false;
  }
  
  isBotActive(userId) {
    if (!this.interactions[userId]) {
      return true;
    }
    return this.interactions[userId].botActive !== false;
  }
  
  setBotActive(userId, active) {
    if (!this.interactions[userId]) {
      this.recordInteraction(userId);
    }
    this.interactions[userId].botActive = active;
    if (active) {
      this.interactions[userId].ignoredMessages = 0;
    }
    this.saveInteractions();
  }
  
  recordBotResponse(userId) {
    if (this.interactions[userId]) {
      this.interactions[userId].lastBotResponse = Date.now();
      this.saveInteractions();
    }
  }
  
  shouldRespondToGeneral(userId) {
    if (!this.interactions[userId]) {
      return true;
    }
    
    const lastResponse = this.interactions[userId].lastBotResponse;
    if (!lastResponse) {
      return true;
    }
    
    const timeSinceLastResponse = Date.now() - lastResponse;
    const FIVE_MINUTES = 5 * 60 * 1000;
    
    return timeSinceLastResponse > FIVE_MINUTES;
  }
  
  incrementIgnoredMessages(userId) {
    if (this.interactions[userId]) {
      this.interactions[userId].ignoredMessages++;
      if (this.interactions[userId].ignoredMessages % 10 === 0) {
        this.saveInteractions();
      }
    }
  }
}

const userTracker = new UserTracker();

// ==================
// FORWARDED MESSAGES MANAGER
// ==================
class ForwardedMessagesManager {
  constructor() {
    this.messages = safeReadJSON(FORWARDED_MESSAGES_FILE, {});
  }
  
  saveMessages() {
    safeWriteJSON(FORWARDED_MESSAGES_FILE, this.messages);
  }
  
  addMessage(messageId, data) {
    this.messages[messageId] = {
      ...data,
      timestamp: Date.now()
    };
    this.saveMessages();
  }
  
  findByStaff(staffNumber, originalMessage) {
    for (const [msgId, data] of Object.entries(this.messages)) {
      if (data.staffNumber === staffNumber && 
          data.originalMessage.includes(originalMessage.substring(0, 50))) {
        return { id: msgId, data };
      }
    }
    return null;
  }
  
  removeMessage(messageId) {
    if (this.messages[messageId]) {
      delete this.messages[messageId];
      this.saveMessages();
      return true;
    }
    return false;
  }
  
  cleanup() {
    const now = Date.now();
    let cleaned = 0;
    
    for (const [msgId, data] of Object.entries(this.messages)) {
      if (now - data.timestamp > FORWARDED_MSG_CLEANUP) {
        delete this.messages[msgId];
        cleaned++;
      }
    }
    
    if (cleaned > 0) {
      this.saveMessages();
      console.log(`🧹 Cleaned ${cleaned} old forwarded messages`);
    }
  }
}

const forwardedMessages = new ForwardedMessagesManager();

// ==================
// LOGGING UTILITIES
// ==================
function logRequest(userId, intent, success, responseTime) {
  try {
    rotateLogFile(REQUEST_LOG_FILE);
    const maskedUserId = userId.replace(/\d{8}/, '****');
    const logEntry = {
      timestamp: new Date().toISOString(),
      userId: maskedUserId,
      intent,
      success,
      responseTime: responseTime + 'ms'
    };
    fs.appendFileSync(REQUEST_LOG_FILE, JSON.stringify(logEntry) + '\n');
  } catch (error) {
    // Silent fail for logging
  }
}

function logError(error, context = '') {
  try {
    rotateLogFile(ERROR_LOG_FILE);
    const logEntry = {
      timestamp: new Date().toISOString(),
      error: error.message,
      stack: error.stack,
      context
    };
    fs.appendFileSync(ERROR_LOG_FILE, JSON.stringify(logEntry) + '\n');
  } catch (err) {
    // Silent fail for logging
  }
}

// ==================
// WHATSAPP CLIENT SETUP (IMPROVED FOR RAILWAY)
// ==================
const client = new Client({
  authStrategy: new LocalAuth({
    dataPath: path.join(DATA_DIR, '.wwebjs_auth')
  }),
  puppeteer: {
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process', // Important for Railway
      '--disable-gpu',
      '--disable-web-security',
      '--disable-features=IsolateOrigins,site-per-process'
    ],
    timeout: 60000
  },
  webVersionCache: {
    type: 'remote',
    remotePath: 'https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html'
  }
});

let isClientReady = false;

client.on("qr", (qr) => {
  console.log("\n📱 SCAN THIS QR CODE WITH WHATSAPP:\n");
  qrcode.generate(qr, { small: true });
  console.log("\n⚠️ QR Code will expire in 60 seconds\n");
  console.log("💡 Tip: For Railway deployment, scan during initial setup\n");
});

client.on("ready", async () => {
  isClientReady = true;
  console.log("\n" + "═".repeat(60));
  console.log("✅ WHATSAPP BOT IS READY!");
  console.log("🏢 Mercantile Produce Brokers - Tea Brokering Assistant");
  console.log("═".repeat(60) + "\n");
  
  await verifyAllSheets();
  
  console.log("🚀 ACTIVE FEATURES:");
  console.log("   ✓ Advanced NLP Intent Classification");
  console.log("   ✓ Real-time Analytics & Tracking");
  console.log("   ✓ Smart Rate Limiting (15 req/min)");
  console.log("   ✓ Message Deduplication");
  console.log("   ✓ Department Forwarding System");
  console.log("   ✓ Auto-reply to Staff Responses");
  console.log("   ✓ Factory Performance Reports");
  console.log("   ✓ Elevation Averages");
  console.log("   ✓ Market Report Downloads");
  console.log("   ✓ Health Monitoring API");
  console.log("   ✓ Persistent Data Storage\n");
  
  const stats = analytics.getStats();
  console.log("📊 CURRENT STATISTICS:");
  console.log(`   - Total Messages: ${stats.total_messages}`);
  console.log(`   - Unique Users: ${stats.unique_users}`);
  console.log(`   - Success Rate: ${100 - parseFloat(stats.error_rate)}%`);
  console.log(`   - Uptime: ${stats.uptime}\n`);
  
  console.log("═".repeat(60) + "\n");
  
  updateHealthStatus();
});

client.on("auth_failure", (msg) => {
  console.error("❌ Authentication failed:", msg);
  logError(new Error("Authentication failure"), "WhatsApp Auth");
});

client.on("disconnected", (reason) => {
  isClientReady = false;
  console.log("⚠️ Client disconnected:", reason);
  logError(new Error(`Disconnected: ${reason}`), "WhatsApp Connection");
  
  // Auto-reconnect after 5 seconds
  setTimeout(() => {
    console.log("🔄 Attempting to reconnect...");
    client.initialize();
  }, 5000);
});

// Initialize client
client.initialize().catch(err => {
  console.error("❌ Failed to initialize client:", err);
  logError(err, "Client Initialization");
});

// ==================
// GOOGLE API SETUP (SECURE)
// ==================
const auth = new google.auth.JWT({
  email: process.env.GOOGLE_CLIENT_EMAIL,
  key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  scopes: [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
  ],
});

const drive = google.drive({ version: "v3", auth });
const sheets = google.sheets({ version: "v4", auth });

// Environment variables
const SHEET_ID = process.env.SHEET_ID;
const FACTORY_SHEET_ID = process.env.FACTORY_SHEET_ID;
const STAFF_SHEET_ID = process.env.STAFF_SHEET_ID;
const ELEVATION_AVG_SHEET_ID = process.env.ELEVATION_AVG_SHEET_ID;
const DRIVE_FOLDER_ID = process.env.DRIVE_FOLDER_ID;

const SHEET_NAME = process.env.SHEET_NAME || "Sheet1";
const FACTORY_SHEET_NAME = process.env.FACTORY_SHEET_NAME || "WES";
const STAFF_SHEET_NAME = process.env.STAFF_SHEET_NAME || "SF01";
const ELEVATION_AVG_SHEET_NAME = process.env.ELEVATION_AVG_SHEET_NAME || "Sheet1";

// Staff directory cache
let staffDirectory = null;
let lastStaffFetch = 0;

// ==================
// VERIFICATION
// ==================
async function verifyAllSheets() {
  console.log("🔍 Verifying Google Sheets & Drive access...\n");

  const sheetsToVerify = [
    { id: SHEET_ID, name: "Elevation Averages", tab: SHEET_NAME },
    { id: FACTORY_SHEET_ID, name: "Factory Performance", tab: FACTORY_SHEET_NAME },
    { id: STAFF_SHEET_ID, name: "Staff Directory", tab: STAFF_SHEET_NAME },
    { id: ELEVATION_AVG_SHEET_ID, name: "Elevation Data", tab: ELEVATION_AVG_SHEET_NAME },
  ];

  for (const sheet of sheetsToVerify) {
    try {
      const response = await sheets.spreadsheets.get({
        spreadsheetId: sheet.id,
        includeGridData: false,
      });
      
      const sheetExists = response.data.sheets.some(
        s => s.properties.title === sheet.tab
      );

      if (sheetExists) {
        console.log(`✅ ${sheet.name}: Found "${sheet.tab}"`);
      } else {
        console.log(`⚠️  ${sheet.name}: Tab "${sheet.tab}" not found`);
      }
    } catch (err) {
      console.error(`❌ ${sheet.name}: Failed - ${err.message}`);
      console.log(`   Share with: ${process.env.GOOGLE_CLIENT_EMAIL}`);
    }
  }

  // Verify Drive folder
  try {
    const folder = await drive.files.get({
      fileId: DRIVE_FOLDER_ID,
      fields: "id, name, mimeType",
    });
    console.log(`✅ Drive Folder: "${folder.data.name}"`);
  } catch (err) {
    console.error(`❌ Drive Folder: Failed - ${err.message}`);
    console.log(`   Share with: ${process.env.GOOGLE_CLIENT_EMAIL}`);
  }

  console.log("\n" + "─".repeat(60) + "\n");
}

// ==================
// STAFF DIRECTORY
// ==================
async function fetchStaffDirectory() {
  const now = Date.now();
  if (staffDirectory && (now - lastStaffFetch) < CACHE_DURATION) {
    return staffDirectory;
  }

  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: STAFF_SHEET_ID,
      range: `${STAFF_SHEET_NAME}!A:C`,
    });

    const rows = res.data.values;
    if (!rows || rows.length <= 1) {
      console.error("❌ No staff data found");
      return {};
    }

    const directory = {};
    
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 2) continue;
      
      const [department, number, name] = row;
      if (!department || !number) continue;
      
      const dept = department.toLowerCase().trim();
      if (!directory[dept]) {
        directory[dept] = [];
      }
      
      let cleanNumber = number.toString().trim();
      
      // Format phone number
      if (cleanNumber.includes('@c.us')) {
        cleanNumber = cleanNumber.replace(/\s+/g, '');
      } else {
        const digitsOnly = cleanNumber.replace(/\D/g, '');
        if (digitsOnly.length > 0) {
          cleanNumber = digitsOnly + '@c.us';
        } else {
          continue;
        }
      }
      
      // Validate phone format
      if (!/^\d+@c\.us$/.test(cleanNumber)) {
        console.warn(`⚠️ Invalid phone format for ${name}: ${cleanNumber}`);
        continue;
      }
      
      directory[dept].push({
        name: name ? name.toString().trim() : "Staff Member",
        number: cleanNumber
      });
    }

    staffDirectory = directory;
    lastStaffFetch = now;
    
    console.log("✅ Staff directory loaded:");
    Object.entries(directory).forEach(([dept, staff]) => {
      console.log(`   - ${dept}: ${staff.length} member(s)`);
    });
    
    return directory;
  } catch (err) {
    console.error("❌ Error fetching staff directory:", err.message);
    logError(err, "fetchStaffDirectory");
    return {};
  }
}

// ==================
// DEPARTMENT ROUTING
// ==================
async function routeToDepartment(msg, department, originalMessage, clientNumber) {
  const directory = await fetchStaffDirectory();
  const dept = department.toLowerCase();
  
  if (!directory[dept] || directory[dept].length === 0) {
    console.error(`⚠️ No staff found for department: ${department}`);
    return false;
  }

  const staff = directory[dept];
  const senderName = msg._data.notifyName || msg._data.pushname || "Client";
  const senderNumber = msg.from.replace('@c.us', '');
  const messageId = `msg_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
  
  const forwardMessage = `
🔔 *NEW ${department.toUpperCase()} REQUEST*
${"═".repeat(42)}

👤 *Client:* ${senderName}
📱 *Number:* +${senderNumber}
🕐 *Time:* ${new Date().toLocaleString('en-LK', { 
    timeZone: 'Asia/Colombo',
    dateStyle: 'medium',
    timeStyle: 'short'
  })}
🆔 *Message ID:* ${messageId}

${"─".repeat(42)}
💬 *Message:*
${originalMessage}
${"═".repeat(42)}

⚡ Reply to this message to send a response to the client.
  `.trim();

  let successCount = 0;
  
  for (const member of staff) {
    try {
      await client.sendMessage(member.number, forwardMessage);
      
      forwardedMessages.addMessage(messageId, {
        clientNumber: msg.from,
        staffNumber: member.number,
        staffName: member.name,
        department: department,
        clientName: senderName,
        originalMessage: originalMessage
      });
      
      console.log(`✅ Forwarded to ${member.name} (${department}) - ID: ${messageId}`);
      successCount++;
    } catch (err) {
      console.error(`❌ Failed to forward to ${member.name}:`, err.message);
      logError(err, `routeToDepartment - ${member.name}`);
    }
  }

  return successCount > 0;
}

// ==================
// STAFF REPLY PROCESSING
// ==================
async function processStaffReply(msg) {
  const staffNumber = msg.from;
  const replyText = msg.body;
  
  if (!msg.hasQuotedMsg) {
    return false;
  }

  try {
    const quotedMsg = await msg.getQuotedMessage();
    const quotedText = quotedMsg.body;
    
    // Find matching forwarded message
    let matchedMessageId = null;
    for (const [msgId, msgData] of Object.entries(forwardedMessages.messages)) {
      if (msgData.staffNumber === staffNumber && 
          quotedText.includes(msgData.originalMessage.substring(0, 50))) {
        matchedMessageId = msgId;
        break;
      }
    }
    
    if (!matchedMessageId) {
      return false;
    }

    const originalData = forwardedMessages.messages[matchedMessageId];
    const clientNumber = originalData.clientNumber;
    
    const clientReply = `
✅ *RESPONSE FROM ${originalData.department.toUpperCase()} DEPARTMENT*
${"═".repeat(42)}

👤 *From:* ${originalData.staffName}
🕐 *Time:* ${new Date().toLocaleString('en-LK', { 
    timeZone: 'Asia/Colombo',
    dateStyle: 'medium',
    timeStyle: 'short'
  })}

${"─".repeat(42)}
💬 *Response:*
${replyText}
${"═".repeat(42)}

Thank you for using Mercantile Produce Brokers!
    `.trim();
    
    await client.sendMessage(clientNumber, clientReply);
    console.log(`✅ Reply sent to client from ${originalData.staffName}`);
    
    forwardedMessages.removeMessage(matchedMessageId);
    
    return true;
  } catch (err) {
    console.error(`❌ Failed to process staff reply:`, err.message);
    logError(err, "processStaffReply");
    return false;
  }
}

// ==================
// FACTORY DATA
// ==================
async function fetchFactoryData(factoryCodes, saleNo) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: FACTORY_SHEET_ID,
      range: `${FACTORY_SHEET_NAME}!A:N`,
    });

    const rows = res.data.values;
    if (!rows || rows.length === 0) {
      console.error("❌ No factory data found");
      return null;
    }

    // Find header row
    let headerRowIndex = 0;
    let headers = [];
    
    for (let i = 0; i < Math.min(5, rows.length); i++) {
      const row = rows[i];
      if (row && row.some(cell => cell && 
          (cell.toString().toUpperCase().includes("YEAR") || 
           cell.toString().toUpperCase().includes("SALENO") || 
           cell.toString().toUpperCase().includes("FCODE")))) {
        headerRowIndex = i;
        headers = row.map(h => h ? h.toString().toUpperCase().trim() : "");
        break;
      }
    }

    if (headers.length === 0) {
      console.error("❌ Header row not found");
      return null;
    }

    // Map column indices
    const salenoIndex = headers.findIndex(h => h.includes("SALENO") || (h.includes("SALE") && h.length < 10));
    const elevationIndex = headers.findIndex(h => h === "ELEVATION");
    const factoryIndex = headers.findIndex(h => h === "FACTORY");
    const fcodeIndex = headers.findIndex(h => h === "FCODE");
    const wqtyIndex = headers.findIndex(h => h === "WQTY");
    const wavgIndex = headers.findIndex(h => h === "WAVG");
    const mqtyIndex = headers.findIndex(h => h === "MQTY");
    const mavgIndex = headers.findIndex(h => h === "MAVG");
    const yqtyIndex = headers.findIndex(h => h === "YQTY");
    const yavgIndex = headers.findIndex(h => h === "YAVG");
    const wrankIndex = headers.findIndex(h => h === "WRANK");
    const mrankIndex = headers.findIndex(h => h === "MRANK");
    const yrankIndex = headers.findIndex(h => h === "YRANK");

    if (fcodeIndex === -1) {
      console.error("❌ FCODE column not found");
      return null;
    }

    const factoryData = [];
    const normalizedSearchCodes = factoryCodes.map(code => 
      code.replace(/\s+/g, '').toUpperCase()
    );
    
    for (let i = headerRowIndex + 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length === 0) continue;
      
      const fcode = row[fcodeIndex] ? row[fcodeIndex].toString().trim() : "";
      if (!fcode) continue;
      
      const normalizedFcode = fcode.replace(/\s+/g, '').toUpperCase();
      
      if (normalizedSearchCodes.includes(normalizedFcode)) {
        // Filter by sale number if provided
        if (saleNo && salenoIndex !== -1) {
          const rowSaleNo = row[salenoIndex] ? row[salenoIndex].toString().trim() : "";
          const rowSaleNoNum = parseInt(rowSaleNo);
          const searchSaleNoNum = parseInt(saleNo);
          
          if (rowSaleNoNum !== searchSaleNoNum) {
            continue;
          }
        }
        
        const getCellValue = (index, defaultValue = "0") => {
          if (index === -1 || !row[index]) return defaultValue;
          const val = row[index].toString().trim();
          return val === "" ? defaultValue : val;
        };
        
        factoryData.push({
          elevation: getCellValue(elevationIndex, "N/A"),
          factory: getCellValue(factoryIndex, "Unknown"),
          fcode: fcode,
          wqty: getCellValue(wqtyIndex, "0"),
          wavg: getCellValue(wavgIndex, "0"),
          mqty: getCellValue(mqtyIndex, "0"),
          mavg: getCellValue(mavgIndex, "0"),
          yqty: getCellValue(yqtyIndex, "0"),
          yavg: getCellValue(yavgIndex, "0"),
          wrank: getCellValue(wrankIndex, "-"),
          mrank: getCellValue(mrankIndex, "-"),
          yrank: getCellValue(yrankIndex, "-"),
        });
      }
    }

    return factoryData.length > 0 ? factoryData : null;
  } catch (err) {
    console.error("❌ Error fetching factory data:", err.message);
    logError(err, "fetchFactoryData");
    return null;
  }
}

async function fetchElevationAveragesForComparison(saleNo) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: ELEVATION_AVG_SHEET_ID,
      range: ELEVATION_AVG_SHEET_NAME,
    });

    const rows = res.data.values;
    if (!rows || rows.length === 0) return {};

    const headers = rows[0].map(h => h ? h.toString().toUpperCase() : "");
    const salenoIndex = headers.findIndex(h => h.includes("SALENO") || h.includes("SALE"));
    const elevationIndex = headers.findIndex(h => h === "ELEVATION");
    const avgIndex = headers.findIndex(h => (h.includes("TOTAL") && h.includes("AVG")) || h === "TOTAL AVG");

    if (salenoIndex === -1 || elevationIndex === -1 || avgIndex === -1) {
      return {};
    }

    const searchSaleNo = parseInt(saleNo).toString();
    const elevationAvgs = {};
    
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length === 0) continue;
      
      const rowSaleNo = row[salenoIndex] ? row[salenoIndex].toString().trim() : "";
      
      if (rowSaleNo === searchSaleNo || rowSaleNo === saleNo) {
        const elevation = row[elevationIndex];
        const avg = row[avgIndex];
        
        if (elevation && avg && avg !== "NULL" && !isNaN(parseFloat(avg))) {
          elevationAvgs[elevation.toString().toUpperCase()] = parseFloat(avg);
        }
      }
    }

    return elevationAvgs;
  } catch (err) {
    console.error("❌ Error fetching elevation averages:", err.message);
    logError(err, "fetchElevationAveragesForComparison");
    return {};
  }
}

async function formatFactoryDataMessage(factoryData, saleNo) {
  const elevationAvgs = saleNo ? await fetchElevationAveragesForComparison(saleNo) : {};
  
  const byElevation = {};
  factoryData.forEach(factory => {
    const elev = factory.elevation.toUpperCase();
    if (!byElevation[elev]) {
      byElevation[elev] = [];
    }
    byElevation[elev].push(factory);
  });

  let message = `🏭 *FACTORY PERFORMANCE REPORT*\n`;
  if (saleNo) {
    message += `📊 Sale No: ${saleNo}\n`;
  }
  message += `${"═".repeat(42)}\n\n`;

  for (const [elevation, factories] of Object.entries(byElevation)) {
    const emoji = ELEVATION_EMOJIS[elevation] || '🍃';
    const elevAvg = elevationAvgs[elevation];
    
    message += `${emoji} *${elevation} ELEVATION*\n`;
    if (elevAvg) {
      message += `📈 Market Avg: Rs. ${parseFloat(elevAvg).toFixed(2)}\n`;
    }
    message += `${"─".repeat(42)}\n\n`;

    factories.forEach((factory, idx) => {
      message += `*${idx + 1}. ${factory.factory}*\n`;
      message += `   Code: ${factory.fcode}\n`;
      message += `${"┈".repeat(42)}\n`;
      
      message += `📅 *WEEKLY PERFORMANCE*\n`;
      const wqty = parseFloat(factory.wqty) || 0;
      const wavg = parseFloat(factory.wavg) || 0;
      message += `   Quantity: ${wqty.toLocaleString()} kg\n`;
      message += `   Average: Rs. ${wavg.toFixed(2)}\n`;
      if (factory.wrank !== "-" && factory.wrank !== "0") {
        message += `   🏆 Rank: #${factory.wrank}\n`;
      }
      
      if (elevAvg && wavg > 0) {
        const diff = wavg - elevAvg;
        const diffPercent = ((diff / elevAvg) * 100).toFixed(1);
        if (diff > 0) {
          message += `   ✅ Above Market: +Rs. ${diff.toFixed(2)} (+${diffPercent}%)\n`;
        } else if (diff < 0) {
          message += `   ⚠️ Below Market: Rs. ${diff.toFixed(2)} (${diffPercent}%)\n`;
        } else {
          message += `   ➖ At Market Average\n`;
        }
      }
      message += `\n`;
      
      message += `📆 *MONTHLY PERFORMANCE*\n`;
      const mqty = parseFloat(factory.mqty) || 0;
      const mavg = parseFloat(factory.mavg) || 0;
      message += `   Quantity: ${mqty.toLocaleString()} kg\n`;
      message += `   Average: Rs. ${mavg.toFixed(2)}\n`;
      if (factory.mrank !== "-" && factory.mrank !== "0") {
        message += `   🏆 Rank: #${factory.mrank}\n`;
      }
      message += `\n`;
      
      message += `📊 *YEARLY PERFORMANCE*\n`;
      const yqty = parseFloat(factory.yqty) || 0;
      const yavg = parseFloat(factory.yavg) || 0;
      message += `   Quantity: ${yqty.toLocaleString()} kg\n`;
      message += `   Average: Rs. ${yavg.toFixed(2)}\n`;
      if (factory.yrank !== "-" && factory.yrank !== "0") {
        message += `   🏆 Rank: #${factory.yrank}\n`;
      }
      
      message += `\n${"─".repeat(42)}\n\n`;
    });
  }

  message += `${"═".repeat(42)}\n`;
  message += `💡 *Legend*\n`;
  message += `✅ Above market | ⚠️ Below market | 🏆 Ranking\n`;
  message += `📅 Week | 📆 Month | 📊 Year\n`;

  return message;
}

// ==================
// ELEVATION AVERAGES
// ==================
async function fetchElevationAverages(saleNo) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A:H`,
    });

    const rows = res.data.values;
    if (!rows || rows.length === 0) {
      return null;
    }

    // Find header row
    let headerRowIndex = 0;
    let headers = [];
    
    for (let i = 0; i < Math.min(3, rows.length); i++) {
      const row = rows[i];
      if (row && row.some(cell => cell && cell.toString().toUpperCase().includes("SALENO"))) {
        headerRowIndex = i;
        headers = row.map(h => h ? h.toString().toUpperCase() : "");
        break;
      }
    }

    if (headers.length === 0) {
      console.error("❌ Header row not found in elevation sheet");
      return null;
    }

    const salenoIndex = headers.findIndex(h => h.includes("SALENO") || h.includes("SALE"));
    const elevationIndex = headers.findIndex(h => h === "ELEVATION");
    const avgIndex = headers.findIndex(h => (h.includes("TOTAL") && h.includes("AVG")) || h === "TOTAL AVG");

    if (salenoIndex === -1 || elevationIndex === -1 || avgIndex === -1) {
      console.error("❌ Required columns not found");
      return null;
    }

    const searchSaleNo = parseInt(saleNo).toString();
    const result = {};
    
    for (let i = headerRowIndex + 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length === 0) continue;
      
      const rowSaleNo = row[salenoIndex] ? row[salenoIndex].toString().trim() : "";
      const rowSaleNoNum = parseInt(rowSaleNo);
      
      if (rowSaleNoNum === parseInt(saleNo)) {
        const elevation = row[elevationIndex];
        const avg = row[avgIndex];
        
        if (elevation && avg && avg !== "NULL") {
          result[elevation.toString().toUpperCase()] = avg.toString();
        }
      }
    }

    if (Object.keys(result).length === 0) {
      return null;
    }

    let message = `📊 *ELEVATION AVERAGES*\n`;
    message += `Sale No: ${saleNo}\n`;
    message += `${"═".repeat(42)}\n\n`;
    
    for (const [elev, avg] of Object.entries(result)) {
      const emoji = ELEVATION_EMOJIS[elev] || '🍃';
      message += `${emoji} ${elev.padEnd(10)} Rs. ${avg}\n`;
    }
    
    message += `\n${"═".repeat(42)}`;

    return message;
  } catch (err) {
    console.error("❌ Error fetching elevation averages:", err.message);
    logError(err, "fetchElevationAverages");
    return null;
  }
}

// ==================
// MARKET REPORTS
// ==================
async function fetchMarketReport(saleNo) {
  try {
    const searchQuery = `'${DRIVE_FOLDER_ID}' in parents and name contains '${saleNo}' and mimeType='application/pdf'`;
    
    const res = await drive.files.list({
      q: searchQuery,
      fields: "files(id, name)",
      spaces: "drive"
    });

    if (!res.data.files || res.data.files.length === 0) return null;

    const file = res.data.files[0];
    const destPath = path.join(DATA_DIR, file.name);

    return new Promise((resolve, reject) => {
      const dest = fs.createWriteStream(destPath);
      
      drive.files.get(
        { fileId: file.id, alt: "media" },
        { responseType: "stream" },
        (err, res) => {
          if (err) {
            console.error("Drive download error:", err);
            logError(err, "fetchMarketReport");
            reject(err);
            return;
          }
          
          res.data
            .on("end", () => {
              console.log("✅ Downloaded:", file.name);
              resolve(destPath);
            })
            .on("error", (err) => {
              logError(err, "fetchMarketReport - stream");
              reject(err);
            })
            .pipe(dest);
        }
      );
    });
  } catch (err) {
    console.error("❌ Error fetching market report:", err.message);
    logError(err, "fetchMarketReport");
    return null;
  }
}

// ==================
// MESSAGE TEMPLATES
// ==================
function getWelcomeMessage() {
  return `
*MIRA – Mercantile Intelligent Response Assistant 💡🤖*
MIRA is a smart AI assistant designed by MPBL IT.

${"═".repeat(42)}

📊 *AVAILABLE SERVICES*

*1️⃣ Factory Performance Data*
   Examples:
   • \`MF 0235 average\`
   • \`MF0235 MF0777 performance\`
   • \`MF 1234 sale 38 data\`

*2️⃣ Elevation Prices*
   Examples:
   • \`elevation sale 38\`
   • \`UH averages sale 38\`

*3️⃣ Market Reports*
   Examples:
   • \`market report sale 38\`
   • \`sale 38 pdf report\`

*4️⃣ Department Contacts*
   • *Valuation*: Appraisals, reports
   • *Accounts*: Tax, VAT, invoices, DOs
   • *IT*: Technical support
   • *Marketing*: General inquiries

${"═".repeat(42)}

💡 *Quick Commands*
   • \`help\` - Show this menu
   • \`contact\` - Contact information
   • \`status\` - Bot statistics
   • \`mute bot\` - Stop bot responses
   • \`unmute bot\` - Resume bot responses

${"═".repeat(42)}

_I won't spam you! I only respond to specific requests._
_This welcome message appears once per day._

Type your request to get started!
  `.trim();
}

function getContactInfo() {
  return `
📞 *CONTACT INFORMATION*
${"═".repeat(42)}

🏢 *Mercantile Produce Brokers Pvt Ltd*
🍃 Built on Trust & Strong Bonds

📧 Email: info@merctea.lk
🌐 Website: www.merctea.lk
📍 133, Jawatta Rd, Colombo 05, Sri Lanka

⏰ *Business Hours*
Mon-Fri: 9:00 AM - 5:00 PM

${"═".repeat(42)}

For urgent matters, contact the relevant
department through this bot.
  `.trim();
}

function getBotStatus() {
  const stats = analytics.getStats();
  return `
📊 *BOT STATUS & STATISTICS*
${"═".repeat(42)}

⏱️ *Uptime:* ${stats.uptime}
📬 *Total Messages:* ${stats.total_messages}
👥 *Unique Users:* ${stats.unique_users}
✅ *Success Rate:* ${(100 - parseFloat(stats.error_rate)).toFixed(1)}%
⚡ *Avg Response:* ${stats.average_response_time}
💾 *Memory:* ${stats.memory_usage.heap_used}

${"─".repeat(42)}

📈 *Popular Requests:*
${stats.popular_intents.map(([intent, count], i) => 
  `${i + 1}. ${intent}: ${count} requests`
).join('\n')}

${"═".repeat(42)}

✅ All systems operational
  `.trim();
}

// ==================
// INPUT SANITIZATION
// ==================
function sanitizeInput(text) {
  if (!text || typeof text !== 'string') return '';
  return text
    .replace(/[<>]/g, '')
    .substring(0, 2000)
    .trim();
}

// ==================
// MAIN MESSAGE HANDLER
// ==================
client.on("message", async (msg) => {
  // Ignore group messages and own messages
  if (msg.from.includes("@g.us") || msg.fromMe) return;

  const userId = msg.from;
  const startTime = Date.now();
  let text = sanitizeInput(msg.body);
  
  try {
    // Rate limiting
    if (!rateLimiter.checkLimit(userId)) {
      const remainingTime = rateLimiter.getRemainingTime(userId);
      const violations = rateLimiter.getViolations(userId);
      
      // Only respond every 5 violations to avoid spam
      if (violations % 5 === 1) {
        await msg.reply(
          `⚠️ *Rate Limit Exceeded*\n\n` +
          `Please wait ${remainingTime} seconds before sending more messages.\n` +
          `This helps us maintain quality service for all users.`
        );
      }
      
      analytics.trackMessage(userId, 'rate_limited', Date.now() - startTime, false);
      return;
    }

    // Message deduplication
    const messageHash = crypto
      .createHash('md5')
      .update(`${userId}_${text}_${msg.timestamp}`)
      .digest('hex');

    if (messageCache.isDuplicate(userId, messageHash)) {
      console.log(`⚠️ Duplicate message from ${userId.substring(0, 15)}...`);
      return;
    }

    console.log(`\n📩 Message from ${userId.substring(0, 15)}...: ${text.substring(0, 50)}...`);

    // Track user interaction
    userTracker.recordInteraction(userId);

    // Classify intent
    const intent = classifier.classify(text);
    const entities = classifier.extractEntities(text);
    const saleNo = entities.sale_number;
    
    console.log(`🎯 Intent: ${intent}`);
    if (Object.values(entities).some(v => v && (Array.isArray(v) ? v.length > 0 : true))) {
      console.log(`🔍 Entities:`, JSON.stringify(entities));
    }

    // Check if this is an unmute command
    const lowerText = text.toLowerCase();
    const isUnmuteCommand = intent === "bot_control" && 
      (lowerText.includes('unmute') || 
       lowerText.includes('activate') || 
       lowerText.includes('resume') || 
       lowerText.includes('enable') ||
       lowerText.includes('start'));

    // Check if bot is muted
    if (!userTracker.isBotActive(userId) && !isUnmuteCommand) {
      console.log(`🔇 Bot muted for ${userId.substring(0, 15)}... - Ignoring message`);
      userTracker.incrementIgnoredMessages(userId);
      return;
    }

    const responseTime = Date.now() - startTime;

    // Check for staff reply
    const isStaffReply = await processStaffReply(msg);
    if (isStaffReply) {
      console.log("✅ Staff reply processed");
      analytics.trackMessage(userId, 'staff_reply', Date.now() - startTime, true);
      logRequest(userId, 'staff_reply', true, Date.now() - startTime);
      return;
    }

    // Handle bot control commands
    if (intent === "bot_control") {
      if (lowerText.includes('stop') || lowerText.includes('mute') || lowerText.includes('pause') || lowerText.includes('disable')) {
        userTracker.setBotActive(userId, false);
        await msg.reply(
          `🔇 *Bot Muted*\n\n` +
          `I will no longer respond to your messages.\n\n` +
          `To reactivate me, send:\n` +
          `• \`activate bot\`\n` +
          `• \`unmute bot\`\n` +
          `• \`resume bot\`\n\n` +
          `_I'll still listen for these commands._`
        );
        analytics.trackMessage(userId, intent, responseTime, true);
        logRequest(userId, 'bot_muted', true, responseTime);
        return;
      } else if (isUnmuteCommand) {
        userTracker.setBotActive(userId, true);
        await msg.reply(
          `🔔 *Bot Activated!*\n\n` +
          `I'm back and ready to assist you.\n\n` +
          `Type *help* to see what I can do.`
        );
        analytics.trackMessage(userId, intent, responseTime, true);
        logRequest(userId, 'bot_activated', true, responseTime);
        return;
      }
    }

    // Handle irrelevant queries
    if (intent === "irrelevant") {
      if (userTracker.shouldRespondToGeneral(userId)) {
        await msg.reply(
          `Thank you for your interest in Mercantile Produce Brokers.\n\n` +
          `This is an automated tea brokering assistant. For HR inquiries, please contact our office directly:\n\n` +
          `📞 +94 112 581 358\n` +
          `📧 info@merctea.lk\n\n` +
          `_If you don't need bot assistance, I'll stay quiet. Type "help" if you need me later._`
        );
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, responseTime, true);
        logRequest(userId, 'irrelevant_query', true, responseTime);
      }
      return;
    }

    // Handle casual conversation
    if (intent === "casual_conversation") {
      console.log(`💬 Casual conversation detected - Not responding`);
      return;
    }

    // Handle commands
    if (intent === "help") {
      await msg.reply(getWelcomeMessage());
      userTracker.recordBotResponse(userId);
      analytics.trackMessage(userId, intent, responseTime, true);
      logRequest(userId, intent, true, responseTime);
      return;
    }

    if (intent === "contact") {
      await msg.reply(getContactInfo());
      userTracker.recordBotResponse(userId);
      analytics.trackMessage(userId, intent, responseTime, true);
      logRequest(userId, intent, true, responseTime);
      return;
    }

    if (intent === "status") {
      await msg.reply(getBotStatus());
      userTracker.recordBotResponse(userId);
      analytics.trackMessage(userId, intent, responseTime, true);
      logRequest(userId, intent, true, responseTime);
      return;
    }

    // Factory queries
    if (intent === "factory_query") {
      const factoryCodes = entities.factory_codes;
      
      if (factoryCodes.length === 0) {
        await msg.reply("⚠️ Please provide valid factory code(s).\n\n*Example:* MF 0235 average");
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, false);
        logRequest(userId, intent, false, Date.now() - startTime);
        return;
      }

      if (factoryCodes.length > 5) {
        await msg.reply("⚠️ Maximum 5 factory codes per request.");
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, false);
        logRequest(userId, intent, false, Date.now() - startTime);
        return;
      }

      await msg.reply(`⏳ Fetching data for: ${factoryCodes.join(', ')}...`);
      
      const factoryData = await fetchFactoryData(factoryCodes, saleNo);
      
      if (factoryData && factoryData.length > 0) {
        const formattedMessage = await formatFactoryDataMessage(factoryData, saleNo);
        await msg.reply(formattedMessage);
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, true);
        logRequest(userId, intent, true, Date.now() - startTime);
      } else {
        await msg.reply(`⚠️ No data found for: ${factoryCodes.join(', ')}${saleNo ? ` (Sale ${saleNo})` : ''}`);
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, false);
        logRequest(userId, intent, false, Date.now() - startTime);
      }
      return;
    }

    // Elevation queries
    if (intent === "elevation_query" && saleNo) {
      await msg.reply("⏳ Fetching elevation data...");
      const averages = await fetchElevationAverages(saleNo);
      if (averages) {
        await msg.reply(averages);
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, true);
        logRequest(userId, intent, true, Date.now() - startTime);
      } else {
        await msg.reply(`⚠️ No elevation data found for Sale ${saleNo}.`);
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, false);
        logRequest(userId, intent, false, Date.now() - startTime);
      }
      return;
    }

    // Market reports
    if (intent === "market_report" && saleNo) {
      await msg.reply("⏳ Fetching market report...");
      const filePath = await fetchMarketReport(saleNo);
      if (filePath) {
        const media = MessageMedia.fromFilePath(filePath);
        await msg.reply(media);
        fs.unlinkSync(filePath);
        await msg.reply("✅ Report sent successfully!");
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, true);
        logRequest(userId, intent, true, Date.now() - startTime);
      } else {
        await msg.reply(`⚠️ Market report for Sale ${saleNo} not found.`);
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, false);
        logRequest(userId, intent, false, Date.now() - startTime);
      }
      return;
    }

    // Department routing
    if (intent === "department_contact") {
      const department = entities.department;
      
      if (!department) {
        await msg.reply(
          `📞 *DEPARTMENT CONTACTS*\n\n` +
          `Please specify which department:\n\n` +
          `• *Valuation* - Appraisals & reports\n` +
          `• *Accounts* - Tax, VAT, invoices\n` +
          `• *IT* - Technical support\n` +
          `• *Marketing* - General inquiries\n\n` +
          `Example: "I need help from accounting"`
        );
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, false);
        logRequest(userId, intent, false, Date.now() - startTime);
        return;
      }

      await msg.reply(`⏳ Connecting to ${department} Department...`);
      const routed = await routeToDepartment(msg, department, text, userId);
      
      if (routed) {
        await msg.reply(
          `✅ *Thank you for your inquiry!*\n\n` +
          `Your request has been forwarded to our *${department} Department*. ` +
          `Our team will review your message and respond shortly.\n\n` +
          `📞 For urgent matters, please call our office directly.`
        );
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, true);
        logRequest(userId, intent, true, Date.now() - startTime);
      } else {
        await msg.reply(
          `⚠️ Unable to route your request to the ${department} Department.\n\n` +
          `Please try again or contact us directly at:\n` +
          `📞 +94 112 581 358\n` +
          `📧 info@merctea.lk`
        );
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, false);
        logRequest(userId, intent, false, Date.now() - startTime);
      }
      return;
    }

    // General/Unknown intent
    if (intent === "general") {
      if (userTracker.shouldSendWelcome(userId)) {
        await msg.reply(getWelcomeMessage());
        userTracker.recordBotResponse(userId);
        analytics.trackMessage(userId, intent, Date.now() - startTime, true);
        logRequest(userId, intent, true, Date.now() - startTime);
      } else {
        if (userTracker.shouldRespondToGeneral(userId)) {
          await msg.reply(
            `👋 I'm here if you need:\n\n` +
            `• 🏭 Factory data\n` +
            `• 📊 Elevation averages\n` +
            `• 📄 Market reports\n` +
            `• 📞 Department contacts\n\n` +
            `Type *help* for more info.\n\n` +
            `_To mute me, send: "mute bot"_`
          );
          userTracker.recordBotResponse(userId);
          analytics.trackMessage(userId, intent, Date.now() - startTime, true);
          logRequest(userId, intent, true, Date.now() - startTime);
        } else {
          console.log(`🤫 Silently ignoring general message (avoiding spam)`);
        }
      }
    }

  } catch (err) {
    const responseTime = Date.now() - startTime;
    console.error("❌ Error handling message:", err.message);
    logError(err, `Message Handler - User: ${userId.substring(0, 15)}`);
    analytics.trackMessage(userId, 'error', responseTime, false);
    logRequest(userId, 'error', false, responseTime);
    
    await msg.reply(
      `⚠️ *An error occurred while processing your request.*\n\n` +
      `Please try again in a moment. If the problem persists, ` +
      `contact our support team directly.\n\n` +
      `📞 Support: +94 112 581 358`
    );
  }
});

// ==================
// HEALTH MONITORING
// ==================
function updateHealthStatus() {
  try {
    const stats = analytics.getStats();
    const health = {
      status: isClientReady ? 'connected' : 'disconnected',
      timestamp: new Date().toISOString(),
      uptime: stats.uptime,
      memory_usage: stats.memory_usage,
      statistics: {
        total_messages: stats.total_messages,
        unique_users: stats.unique_users,
        successful_requests: stats.successful_requests,
        failed_requests: stats.failed_requests,
        error_rate: stats.error_rate,
        avg_response_time: stats.average_response_time
      },
      rate_limiter: {
        active_users: rateLimiter.userLimits.size
      },
      cache: {
        message_cache_size: Object.keys(messageCache.cache).length,
        forwarded_messages: Object.keys(forwardedMessages.messages).length,
        staff_directory_loaded: staffDirectory !== null
      }
    };
    
    safeWriteJSON(HEALTH_FILE, health);
  } catch (error) {
    console.error('⚠️ Failed to update health status:', error.message);
  }
}

function loadHealthStatus() {
  return safeReadJSON(HEALTH_FILE, {
    status: 'unknown',
    timestamp: new Date().toISOString()
  });
}

// ==================
// PERIODIC MAINTENANCE TASKS
// ==================

// Refresh staff directory every 5 minutes
setInterval(async () => {
  console.log("🔄 Refreshing staff directory cache...");
  try {
    await fetchStaffDirectory();
  } catch (error) {
    console.error("❌ Failed to refresh staff directory:", error.message);
    logError(error, "Staff Directory Refresh");
  }
}, CACHE_DURATION);

// Cleanup old forwarded messages every hour
setInterval(() => {
  console.log("🧹 Cleaning up old forwarded messages...");
  try {
    forwardedMessages.cleanup();
  } catch (error) {
    console.error("❌ Cleanup failed:", error.message);
    logError(error, "Forwarded Messages Cleanup");
  }
}, 60 * 60 * 1000);

// Cleanup message cache every 30 minutes
setInterval(() => {
  console.log("🧹 Cleaning up message cache...");
  try {
    messageCache.cleanup();
  } catch (error) {
    console.error("❌ Cache cleanup failed:", error.message);
    logError(error, "Message Cache Cleanup");
  }
}, 30 * 60 * 1000);

// Cleanup rate limiter every 15 minutes
setInterval(() => {
  console.log("🧹 Cleaning up rate limiter...");
  try {
    rateLimiter.cleanup();
  } catch (error) {
    console.error("❌ Rate limiter cleanup failed:", error.message);
  }
}, 15 * 60 * 1000);

// Update health status every minute
setInterval(() => {
  updateHealthStatus();
}, 60 * 1000);

// Save analytics every 5 minutes
setInterval(() => {
  console.log("💾 Saving analytics...");
  try {
    analytics.saveAnalytics();
    userTracker.saveInteractions();
    messageCache.saveCache();
    forwardedMessages.saveMessages();
  } catch (error) {
    console.error("❌ Save failed:", error.message);
  }
}, 5 * 60 * 1000);

// Analytics reporting every 10 minutes
setInterval(() => {
  try {
    const stats = analytics.getStats();
    console.log("\n📊 LIVE ANALYTICS:");
    console.log(`   - Total Messages: ${stats.total_messages}`);
    console.log(`   - Unique Users: ${stats.unique_users}`);
    console.log(`   - Success Rate: ${(100 - parseFloat(stats.error_rate)).toFixed(1)}%`);
    console.log(`   - Avg Response: ${stats.average_response_time}`);
    console.log(`   - Uptime: ${stats.uptime}`);
    console.log(`   - Memory: ${stats.memory_usage.heap_used}`);
    if (stats.popular_intents.length > 0) {
      console.log(`   - Top Intent: ${stats.popular_intents[0][0]} (${stats.popular_intents[0][1]} times)`);
    }
    console.log("");
  } catch (error) {
    console.error("❌ Analytics reporting failed:", error.message);
  }
}, 10 * 60 * 1000);

// ==================
// GRACEFUL SHUTDOWN
// ==================
process.on('SIGINT', async () => {
  console.log('\n⚠️ Shutting down gracefully...');
  
  try {
    // Save all data
    console.log('💾 Saving all data...');
    analytics.saveAnalytics();
    messageCache.saveCache();
    userTracker.saveInteractions();
    forwardedMessages.saveMessages();
    updateHealthStatus();
    
    // Destroy client
    console.log('🔌 Disconnecting WhatsApp...');
    await client.destroy();
    
    console.log('✅ Shutdown complete');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during shutdown:', error.message);
    process.exit(1);
  }
});

process.on('SIGTERM', async () => {
  console.log('\n⚠️ SIGTERM received, shutting down...');
  
  try {
    analytics.saveAnalytics();
    messageCache.saveCache();
    userTracker.saveInteractions();
    forwardedMessages.saveMessages();
    updateHealthStatus();
    await client.destroy();
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during shutdown:', error.message);
    process.exit(1);
  }
});

process.on('uncaughtException', (error) => {
  console.error('❌ UNCAUGHT EXCEPTION:', error.message);
  logError(error, 'Uncaught Exception');
  
  // Try to save data before crash
  try {
    analytics.saveAnalytics();
    updateHealthStatus();
  } catch (e) {
    console.error('Failed to save on crash:', e.message);
  }
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ UNHANDLED REJECTION:', reason);
  logError(new Error(String(reason)), 'Unhandled Rejection');
});

// ==================
// STARTUP
// ==================
console.log("\n" + "═".repeat(60));
console.log("🚀 MPB WHATSAPP BOT - PRODUCTION READY v2.0");
console.log("═".repeat(60) + "\n");

console.log("📋 CONFIGURATION:");
console.log(`   ✓ Main Sheet: ${SHEET_ID ? 'Configured' : '✗ Missing'}`);
console.log(`   ✓ Factory Sheet: ${FACTORY_SHEET_ID ? 'Configured' : '✗ Missing'}`);
console.log(`   ✓ Staff Sheet: ${STAFF_SHEET_ID ? 'Configured' : '✗ Missing'}`);
console.log(`   ✓ Elevation Sheet: ${ELEVATION_AVG_SHEET_ID ? 'Configured' : '✗ Missing'}`);
console.log(`   ✓ Drive Folder: ${DRIVE_FOLDER_ID ? 'Configured' : '✗ Missing'}`);
console.log(`   ✓ Google Auth: ${process.env.GOOGLE_CLIENT_EMAIL ? 'Configured' : '✗ Missing'}`);

console.log("\n🔧 FEATURES:");
console.log("   ✓ Advanced NLP Intent Classification");
console.log("   ✓ Real-time Analytics & Tracking");
console.log("   ✓ Smart Rate Limiting (15 req/min)");
console.log("   ✓ Message Deduplication");
console.log("   ✓ Department Routing & Auto-reply");
console.log("   ✓ Factory Performance Reports");
console.log("   ✓ Elevation Averages");
console.log("   ✓ Market Report Downloads");
console.log("   ✓ Health Monitoring API");
console.log("   ✓ Error Logging & Recovery");
console.log("   ✓ Graceful Shutdown");
console.log("   ✓ Auto-reconnect on Disconnect");
console.log("   ✓ Persistent Storage");

console.log("\n📁 DATA DIRECTORIES:");
console.log(`   - Data: ${DATA_DIR}`);
console.log(`   - Logs: ${LOGS_DIR}`);

console.log("\n🌐 HEALTH SERVER:");
console.log(`   - Port: ${PORT}`);
console.log(`   - Health: http://localhost:${PORT}/health`);
console.log(`   - Stats: http://localhost:${PORT}/stats`);

console.log("\n" + "═".repeat(60) + "\n");
console.log("⏳ Initializing WhatsApp connection...\n");

// Export for testing
module.exports = {
  client,
  classifier,
  analytics,
  rateLimiter,
  messageCache,
  userTracker,
  forwardedMessages,
  app
};
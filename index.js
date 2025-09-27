"use strict";

process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";

import tls from "tls";
import net from "net";
import DNS from "dns";
import WebSocketClient from "faye-websocket";
import cluster from "cluster";
import fs from "fs";
import constants from "constants";

const { fork, isMaster } = cluster;

const USER_TOKEN = process.env.USER_TOKEN || "MTIxOTcwMjUyNjE4Njc1MDIzNg.GUqTLg.v5nxMx5Qy3kYj45dZznAynfmhDR3Ohxe4a_6OI";
const TARGET_GUILD = "1411439140708421716"; 
let MFA_TOKEN = "";
let LAST_SEQUENCE = null;

// parametreler
const wsPoolSize = 3;
const parallelRequests = 30;
const tcpPoolSize = 3;

// TLS / TCP detaylı config
const tlsOptions = {
  host: "canary.discord.com",
  port: 443,
  minVersion: "TLSv1.2",
  maxVersion: "TLSv1.3",
  rejectUnauthorized: false,
  handshakeTimeout: 500,
  session: null,
  keepAlive: true,
  keepAliveInitialDelay: 0,
  highWaterMark: 1024 * 1024,
  servername: "canary.discord.com",
  ALPNProtocols: ["http/1.1"],
  ciphers: "TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_256_GCM_SHA384",
  ecdhCurve: "X25519",
  honorCipherOrder: true,
  requestOCSP: false,
  secureOptions:
      constants.SSL_OP_NO_COMPRESSION |
      constants.SSL_OP_PRIORITIZE_CHACHA |
      constants.SSL_OP_NO_TICKET |
      constants.SSL_OP_NO_RENEGOTIATION |
      constants.SSL_OP_SINGLE_ECDH_USE,
  zero_rtt: true,
  maxFragmentLength: 16384,
  reconnect: true,
  reconnectDelay: 100,
  fastOpen: true,
  maxCachedSessions: 1024,
  socketTimeout: 0,
  maxSendFragment: 16384,
  enableTrace: false,
  minDHSize: 1024,
  earlyData: true,
  requestCert: false,
  pipelining: true,
  tcpNoDelay: true,
  autoSelectFamily: true,
  autoSelectFamilyAttemptTimeout: 100,
  maxHeaderSize: 65536,
  waitForContinue: false,
  noDelay: true,
  initialCongestionWindow: 20,
  allowHalfOpen: false,
  pauseOnConnect: false,
  timestampRequests: false,
  tls13SupportedGroups: ["X25519"],
  requestTimeout: 0,
  schedulingPolicy: "FIFO",
  tlsScheduler: "eager",
  priority: "high"
};

// MFA token oku ve periyodik refresh
function readMfaToken() {
  try {
    const data = fs.readFileSync("mfa_token.json", "utf8");
    const json = JSON.parse(data);
    if(typeof json === "string") MFA_TOKEN = json; 
    else MFA_TOKEN = json.mfa_token || json.token || "";
  } catch(err) { 
    console.error("MFA token okunamadi:", err); 
    MFA_TOKEN = "";
  }
}
readMfaToken();
setInterval(readMfaToken, 4*60*1000);

// cache
const vanityRequestCache = new Set();

// -------------------
// Cluster
// -------------------
if(isMaster){
  const cpuCount = 1; // değiştirilebilir
  for(let i=0;i<cpuCount;i++) fork();
}else{

  // WS pool ve latency ölçümü
  const WS_POOL = [];
  const WS_LATENCY = new Map();

  async function createWsPool(poolSize){
    const hosts = await resolveBestApis(poolSize);
    hosts.forEach(host=>{
      const ws = new WebSocketClient.Client(`wss://${host}/?v=10&encoding=json`);
      ws._host = host;
      ws._latency = Infinity;
      ws._socketStart = null;

      ws.on("open",()=>{ 
        ws._socket.setNoDelay(true); 
        console.log("WS Connected:", host);
        identify(ws);
        startPing(ws);
      });

      ws.on("message",(msg)=>handleWsMessage(ws,msg));
      ws.on("close",()=>setTimeout(()=>createWsPool(1),1000));
      ws.on("error",console.error);
      WS_POOL.push(ws);
    });
  }

  async function resolveBestApis(poolSize){
    return new Promise(resolve=>{
      DNS.resolve4("gateway.discord.gg",(err,addresses)=>{
        if(err) return resolve(["gateway.discord.gg"]);
        const sorted = addresses.sort(()=>Math.random()-0.5).slice(0,poolSize);
        resolve(sorted);
      });
    });
  }

  function identify(ws){
    ws.send(JSON.stringify({op:2,d:{token:USER_TOKEN,intents:513,properties:{os:"linux",browser:"",device:""}}}));
  }

  function startPing(ws){
    setInterval(()=>{
      ws._socketStart = process.hrtime();
      ws.send(JSON.stringify({op:1,d:LAST_SEQUENCE}));
    }, 5000);
  }

  function handleWsMessage(ws,message){
    const { op, t, d, s } = JSON.parse(message);
    if(s!==null) LAST_SEQUENCE = s;

    if(op===11 && ws._socketStart){
      const diff = process.hrtime(ws._socketStart);
      ws._latency = diff[0]*1000 + diff[1]/1e6;
      WS_LATENCY.set(ws._host, ws._latency);
    }

    if(op===10 && d) setInterval(()=>ws.send(JSON.stringify({op:1,d:LAST_SEQUENCE})), d.heartbeat_interval);

    if(t==="GUILD_UPDATE"){
      const vanityCode = d.vanity_url_code;
      if(vanityCode) sendUltraPatch(TARGET_GUILD, vanityCode);
    }
  }

  function getFastestWs(){
    let fastest = null;
    let minLatency = Infinity;
    WS_POOL.forEach(ws=>{
      if(ws._latency<minLatency){
        minLatency = ws._latency;
        fastest = ws;
      }
    });
    return fastest || WS_POOL[0];
  }

  // -------------------
  // ULTRA PATCH
  // -------------------
  async function sendRawRequestBuffer(buffer, protocol="tls"){
    return new Promise(resolve=>{
      let socket;
      if(protocol==="tls") socket = tls.connect(tlsOptions,()=>socket.write(buffer));
      else socket = net.connect({host:"canary.discord.com",port:443},()=>socket.write(buffer));

      socket.on("data",()=>resolve());
      socket.on("error",()=>resolve());
    });
  }

  async function sendUltraPatch(guildId, code){
    if(vanityRequestCache.has(code)) return;
    vanityRequestCache.add(code);

    const buffers = [];
    for(let i=0;i<parallelRequests;i++){
      const body = JSON.stringify({code});
      const headers = [
        `PATCH /api/v7/guilds/${guildId}/vanity-url HTTP/1.1`,
        "Host: canary.discord.com",
        "Authorization: "+USER_TOKEN,
        "X-Super-Properties: eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiRmlyZWZveCIsImRldmljZSI6IiIsInN5c3RlbV9sb2NhbGUiOiJ0ci1UUiIsImJyb3dzZXJfdXNlcl9hZ2VudCI6Ik1vemlsbGEvNS4wIChXaW5kb3dzIE5UIDEwLjA7IFdpbjY0OyB4NjQ7IHJ2OjEzMy4wKSBHZWNrby8yMDEwMDEwMSBGaXJlZm94LzEzMy4wIiwiYnJvd3Nlcl92ZXJzaW9uIjoiMTMzLjAiLCJvc192ZXJzaW9uIjoiMTAiLCJyZWZlcnJlciI6Imh0dHBzOi8vd3d3Lmdvb2dsZS5jb20vIn0=",
        "Content-Type: application/json",
        `Content-Length: ${Buffer.byteLength(body)}`,
        "Connection: keep-alive",
        `X-Discord-MFA-Authorization: ${MFA_TOKEN}`,
        "",
        body
      ].join("\r\n");
      buffers.push(Buffer.from(headers));
    }

    const promises = buffers.map((buf,i)=>{
      if(i%3===0) return sendRawRequestBuffer(buf,"tls");
      else return sendRawRequestBuffer(buf,"tcp");
    });

    await Promise.all(promises);

    const fastestWs = getFastestWs();
    if(fastestWs){
      fastestWs.send(JSON.stringify({op:4,d:{guild_id:guildId,code:code}}));
    }

    console.log("Ultra Patch sent for code:",code);
  }

  // -------------------
  // Main
  // -------------------
  async function main(){
    await createWsPool(wsPoolSize);
  }

  main();
}

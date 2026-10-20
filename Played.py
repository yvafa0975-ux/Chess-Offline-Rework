"""Chess Offline — Pygame UI + FastAPI LAN server.
Install: python -m pip install pygame python-chess fastapi uvicorn requests
Run:     python ChessPygame_FastAPI.py
"""
import json, os, random, sys, time, threading, math, socket, ast, operator, array, multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from queue import Queue, Empty

try:
    import pygame
    import chess
    import requests
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
    import uvicorn
except ImportError as exc:
    print(f"Не найдена зависимость: {exc.name}. Установи: python -m pip install pygame python-chess fastapi uvicorn requests")
    raise SystemExit(1)

APP = "Chess Offline — Pygame"
MOVE_ANIMATION_SECONDS = 0.22
BG=(18,24,32); PANEL=(31,41,53); PANEL2=(43,56,71); TEXT=(239,243,248); MUTED=(157,171,188)
ACCENT=(129,175,102); ACCENT_DARK=(83,125,74); GREEN=(70,214,154); LIGHT=(235,236,208); DARK=(119,149,101)
SELECT=(246,215,91); RED=(213,91,91); BLUE=(91,143,202); WHITE=(250,248,238); BLACK=(35,39,42)
SYMBOL={"K":"♔","Q":"♕","R":"♖","B":"♗","N":"♘","P":"♙","k":"♚","q":"♛","r":"♜","b":"♝","n":"♞","p":"♟"}


def data_file():
    if os.name == 'nt': root=Path(os.environ.get('APPDATA',str(Path.home())))
    elif sys.platform == 'darwin': root=Path.home()/'Library'/'Application Support'
    else: root=Path(os.environ.get('XDG_CONFIG_HOME',str(Path.home())/'.config'))
    folder=root/'ChessOffline'; folder.mkdir(parents=True,exist_ok=True); return folder/'profile.json'
SAVE=data_file()
DEFAULT={"nickname":"Player","elo":0,"games":0,"wins":0,"losses":0,"draws":0,"level":2,"mode":"Тренировка","fen":"","rated_active":False,"level_elo":{str(i):0 for i in range(1,9)},"verified":False,"verified_before":False}
def fresh_profile():
    # JSON round-trip also copies nested values such as level_elo.
    return json.loads(json.dumps(DEFAULT, ensure_ascii=False))
def load_profile():
    try:
        d=json.loads(SAVE.read_text(encoding='utf-8')); out=fresh_profile(); out.update(d if isinstance(d,dict) else {})
        if not isinstance(out.get('level_elo'),dict): out['level_elo']={str(i):0 for i in range(1,9)}
        for i in range(1,9): out['level_elo'].setdefault(str(i),0)
        return out
    except Exception: return fresh_profile()
def save_profile(p):
    try: SAVE.write_text(json.dumps(p,ensure_ascii=False,indent=2),encoding='utf-8')
    except OSError: pass

VAL = {1:100, 2:320, 3:330, 4:500, 5:900, 6:20000}

def evaluate(board):
    score = 0
    for ptype, value in VAL.items():
        score += len(board.pieces(ptype, chess.WHITE))*value
        score -= len(board.pieces(ptype, chess.BLACK))*value
    for sq in (chess.D4, chess.E4, chess.D5, chess.E5):
        p = board.piece_at(sq)
        if p: score += 16 if p.color else -16
    return score if board.turn else -score

def search(board, depth, alpha, beta, deadline):
    if time.monotonic() > deadline: raise TimeoutError
    if board.is_checkmate(): return -100000, None
    if board.is_game_over(claim_draw=True): return 0, None
    if depth == 0: return evaluate(board), None
    best, bestmove = -10**9, None
    moves = list(board.legal_moves)
    random.shuffle(moves)
    moves.sort(key=lambda m: board.is_capture(m) or bool(m.promotion), reverse=True)
    for m in moves:
        board.push(m)
        try: score, _ = search(board, depth-1, -beta, -alpha, deadline)
        except TimeoutError:
            board.pop()
            raise
        board.pop()
        score = -score
        if score > best: best, bestmove = score, m
        alpha = max(alpha, score)
        if alpha >= beta: break
    return best, bestmove

def bot_move(board, level):
    """A deliberately beginner-friendly bot with occasional imperfect moves."""
    moves = list(board.legal_moves)
    if not moves: return None
    level = max(1, min(8, int(level)))
    # All levels are softened: lower levels choose randomly often, while even
    # the strongest levels search only briefly and can make a casual mistake.
    random_chance = {1:1.00, 2:.82, 3:.68, 4:.55, 5:.43, 6:.32, 7:.24, 8:.18}[level]
    if random.random() < random_chance:
        return random.choice(moves)
    depth = {1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 7:2, 8:2}[level]
    seconds = {1:.05, 2:.06, 3:.08, 4:.10, 5:.12, 6:.15, 7:.22, 8:.30}[level]
    try:
        _, m = search(board.copy(), depth, -10**9, 10**9, time.monotonic()+seconds)
        return m if m in board.legal_moves else random.choice(moves)
    except TimeoutError:
        return random.choice(moves)


def bot_move_uci_from_fen(fen, level):
    """Run CPU-bound engine work in a separate process so it cannot stutter Pygame."""
    local_board = chess.Board(fen)
    move = bot_move(local_board, level)
    return move.uci() if move else None


# ------------------------- FastAPI authoritative LAN room -------------------------
api=FastAPI(title="Chess Offline LAN", docs_url=None, redoc_url=None)
LAN_LOCK=threading.RLock(); LAN_BOARD=chess.Board(); LAN_PLAYERS=0; LAN_LAST=""; LAN_REV=0; LAN_STARTED=False; LAN_RESULT=''
class MovePayload(BaseModel):
    uci: str
    player: str = "guest"
class ResetPayload(BaseModel):
    pass
class ResignPayload(BaseModel):
    player: str = 'guest'
@api.get('/health')
def health(): return {"ok":True,"service":"Chess Offline LAN"}
@api.get('/state')
def lan_state():
    with LAN_LOCK:
        ended=bool(LAN_RESULT) or LAN_BOARD.is_game_over(claim_draw=True)
        result=LAN_RESULT or (LAN_BOARD.result(claim_draw=True) if LAN_BOARD.is_game_over(claim_draw=True) else '')
        return {"fen":LAN_BOARD.fen(),"moves":[m.uci() for m in LAN_BOARD.move_stack],"last_move":LAN_LAST,"revision":LAN_REV,"players":LAN_PLAYERS,"started":LAN_STARTED,"turn":"white" if LAN_BOARD.turn else "black","game_over":ended,"result":result}
@api.post('/join')
def lan_join():
    global LAN_PLAYERS, LAN_STARTED
    with LAN_LOCK:
        if LAN_PLAYERS >= 2: raise HTTPException(409,"Комната уже заполнена")
        LAN_PLAYERS+=1; LAN_STARTED=LAN_PLAYERS>=2
        return {"ok":True,"players":LAN_PLAYERS,"color":"black" if LAN_PLAYERS==2 else "white"}
@api.post('/move')
def lan_move(payload:MovePayload):
    global LAN_LAST, LAN_REV
    try: move=chess.Move.from_uci(payload.uci)
    except ValueError: raise HTTPException(400,"Некорректный ход")
    with LAN_LOCK:
        if LAN_PLAYERS < 2: raise HTTPException(409,"Ожидается второй игрок")
        if LAN_RESULT or LAN_BOARD.is_game_over(claim_draw=True): raise HTTPException(409,"Партия уже завершена")
        expected='host' if LAN_BOARD.turn==chess.WHITE else 'guest'
        if payload.player != expected: raise HTTPException(403,"Сейчас ход другого игрока")
        if move not in LAN_BOARD.legal_moves: raise HTTPException(400,"Недопустимый ход")
        LAN_BOARD.push(move); LAN_LAST=move.uci(); LAN_REV+=1
        return {"ok":True,"revision":LAN_REV,"fen":LAN_BOARD.fen()}
@api.post('/resign')
def lan_resign(payload:ResignPayload):
    global LAN_REV, LAN_LAST, LAN_RESULT
    with LAN_LOCK:
        if payload.player not in ('host','guest'): raise HTTPException(400,'Неизвестный игрок')
        if LAN_RESULT or LAN_BOARD.is_game_over(claim_draw=True): raise HTTPException(409,'Партия уже завершена')
        LAN_RESULT='0-1' if payload.player=='host' else '1-0'
        LAN_LAST=''; LAN_REV+=1
        return {'ok':True,'result':LAN_RESULT,'revision':LAN_REV}

@api.post('/reset')
def lan_reset():
    global LAN_BOARD, LAN_LAST, LAN_REV, LAN_PLAYERS, LAN_STARTED, LAN_RESULT
    with LAN_LOCK:
        LAN_BOARD=chess.Board(); LAN_LAST=""; LAN_RESULT=''; LAN_REV+=1; LAN_PLAYERS=0; LAN_STARTED=False
        return {"ok":True}

class ChessApp:
    def __init__(self):
        try: pygame.mixer.pre_init(22050, -16, 1, 512)
        except Exception: pass
        pygame.init(); pygame.display.set_caption(APP)
        self.W,self.H=1200,780; self.screen=pygame.display.set_mode((self.W,self.H),pygame.RESIZABLE)
        self.clock=pygame.time.Clock(); self.fonts={}; self.p=load_profile(); self.p.setdefault('level_elo',{str(i):0 for i in range(1,9)}); self.nickname=self.p.get('nickname') or 'Player'
        self.board=chess.Board(self.p.get('fen') or None); self.mode='home'; self.game_mode=self.p.get('mode','Тренировка')
        self.level=max(1,min(8,int(self.p.get('level',2)))); self.selected=None; self.targets=[]; self.flipped=False; self.levels_for_mode=None
        self.notice='Добро пожаловать!'; self.notice_until=0; self.thinking=False; self.net_role=''; self.net_url=''; self.net_color=chess.WHITE
        self.net_connected=False; self.net_joined=False; self.net_revision=-1; self.net_queue=Queue(); self.net_stop=threading.Event()
        self.server=None; self.server_thread=None; self.join_text='127.0.0.1'; self.input_active=False; self.rating_game=False
        self.history=[]; self.last_move=None; self.running=True; self.screen_name='Chess Offline'
        self.home_buttons=[]; self.game_buttons=[]; self._active_buttons=[]; self.overlay=None; self.overlay_input=''; self.result_seen=False
        self.anim=None; self.piece_surfaces={}; self.board_surfaces={}; self.text_surfaces={}; self.game_result_override=''; self.game_finished=False; self.calc_expr=''; self.calc_result=''; self.calc_buttons=[]; self.resigned=False; self.promotion_pending=None
        self.result_overlay=None; self.result_particles=[]; self.result_shade=None; self.delete_confirm=False; self.delete_shade=None; self.sound_bank={}; self._cursor_is_hand=None; self._init_sounds()
        # The chess engine is CPU-bound Python. Running it in a thread competes for the GIL
        # and can make the animation look like 10-15 FPS; isolate it in another process.
        self.bot_executor=ProcessPoolExecutor(max_workers=1); self.bot_future=None; self.bot_due=0.0
        self.pending_bot_request=None; self.pending_bot_after=0.0
        if not self.p.get('nickname'): self.mode='profile_setup'

    def font(self,size,bold=False):
        key=(size,bold)
        if key not in self.fonts: self.fonts[key]=pygame.font.SysFont(['dejavusans','segoeui','arial'],size,bold=bold)
        return self.fonts[key]
    def piece_font(self,size):
        key=('chess_piece',size)
        if key not in self.fonts:
            # Segoe UI Symbol supports proper chess glyphs on Windows; fallback to a Unicode font.
            self.fonts[key]=pygame.font.SysFont('DejaVu Sans,Segoe UI Symbol,Noto Sans Symbols,Arial Unicode MS',size)
        return self.fonts[key]
    def piece_sprite(self, symbol, size):
        """Cached, font-independent chess piece silhouettes; no circular background."""
        key=(symbol,size)
        cached=self.piece_surfaces.get(key)
        if cached is not None: return cached
        scale=2; w=100*scale; surf=pygame.Surface((w,w),pygame.SRCALPHA)
        is_white=symbol.isupper()
        fill=(244,241,221) if is_white else (36,41,44)
        edge=(35,43,35) if is_white else (239,239,221)
        shadow=(0,0,0,55)
        def poly(points,color,dx=0,dy=0):
            pts=[(int(x*scale+dx),int(y*scale+dy)) for x,y in points]
            pygame.draw.polygon(surf,color,pts)
            pygame.draw.aalines(surf,color,True,pts)
        def line(points,color,width=2):
            pts=[(int(x*scale),int(y*scale)) for x,y in points]
            pygame.draw.lines(surf,color,False,pts,max(1,int(width*scale)))
        def circ(x,y,r,color):
            pygame.draw.circle(surf,color,(int(x*scale),int(y*scale)),int(r*scale))
            pygame.draw.circle(surf,edge,(int(x*scale),int(y*scale)),int(r*scale),max(1,scale))
        # Drop shadow offsets are small and only behind the silhouette.
        base=[(22,87),(28,80),(35,77),(65,77),(72,80),(78,87),(78,91),(22,91)]
        # shadow first
        poly([(x+1.5,y+1.5) for x,y in base],shadow)
        poly(base,fill); line(base+[base[0]],edge,1.6)
        kind=symbol.upper()
        if kind=='P':
            circ(50,25,9,fill)
            poly([(42,36),(58,36),(61,48),(67,76),(33,76),(39,48)],fill)
            line([(42,36),(58,36),(61,48),(67,76),(33,76),(39,48),(42,36)],edge,1.7)
        elif kind=='R':
            poly([(31,35),(69,35),(66,77),(34,77)],fill); line([(31,35),(69,35),(66,77),(34,77),(31,35)],edge,1.8)
            poly([(27,20),(38,20),(38,27),(46,27),(46,20),(54,20),(54,27),(62,27),(62,20),(73,20),(73,38),(27,38)],fill)
            line([(27,20),(38,20),(38,27),(46,27),(46,20),(54,20),(54,27),(62,27),(62,20),(73,20),(73,38),(27,38),(27,20)],edge,1.8)
        elif kind=='N':
            poly([(30,77),(35,61),(31,49),(34,38),(42,27),(56,21),(62,31),(72,37),(66,44),(56,45),(62,55),(68,77)],fill)
            line([(30,77),(35,61),(31,49),(34,38),(42,27),(56,21),(62,31),(72,37),(66,44),(56,45),(62,55),(68,77),(30,77)],edge,2)
            circ(51,33,2.2,edge)
            line([(40,47),(49,43)],edge,1.2)
        elif kind=='B':
            circ(50,23,5,fill)
            poly([(50,30),(62,44),(59,54),(68,76),(32,76),(41,54),(38,44)],fill)
            line([(50,30),(62,44),(59,54),(68,76),(32,76),(41,54),(38,44),(50,30)],edge,1.8)
            line([(42,45),(56,33)],edge,2)
        elif kind=='Q':
            poly([(32,37),(68,37),(64,51),(70,76),(30,76),(36,51)],fill)
            line([(32,37),(68,37),(64,51),(70,76),(30,76),(36,51),(32,37)],edge,1.8)
            poly([(27,36),(21,21),(39,30),(50,14),(61,30),(79,21),(73,36)],fill)
            line([(27,36),(21,21),(39,30),(50,14),(61,30),(79,21),(73,36),(27,36)],edge,1.8)
            for x,y in [(21,19),(50,12),(79,19)]: circ(x,y,3,fill)
        else: # King
            poly([(32,38),(68,38),(64,52),(70,76),(30,76),(36,52)],fill)
            line([(32,38),(68,38),(64,52),(70,76),(30,76),(36,52),(32,38)],edge,1.8)
            pygame.draw.rect(surf,fill,pygame.Rect(46*scale,10*scale,8*scale,25*scale))
            pygame.draw.rect(surf,edge,pygame.Rect(46*scale,10*scale,8*scale,25*scale),max(1,scale))
            pygame.draw.rect(surf,fill,pygame.Rect(38*scale,17*scale,24*scale,7*scale))
            pygame.draw.rect(surf,edge,pygame.Rect(38*scale,17*scale,24*scale,7*scale),max(1,scale))
        cached=pygame.transform.smoothscale(surf,(size,size)).convert_alpha()
        self.piece_surfaces[key]=cached
        return cached

    def _init_sounds(self):
        """Synthesize short UI/chess sounds at startup; no external files are needed."""
        try:
            if not pygame.mixer.get_init(): pygame.mixer.init(22050, -16, 1, 512)
            rate=22050
            def tone(notes, duration=0.12, volume=0.23, wave='sine'):
                samples=array.array('h')
                count=int(rate*duration)
                for i in range(count):
                    t=i/rate; fade=min(1.0, i/(rate*.012), (count-i)/(rate*.035))
                    value=0.0
                    for freq,weight in notes:
                        phase=2*math.pi*freq*t
                        if wave=='soft': v=math.sin(phase)+0.22*math.sin(phase*2)
                        else: v=math.sin(phase)
                        value += weight*v
                    samples.append(int(max(-1,min(1,value*volume*fade))*32767))
                return pygame.mixer.Sound(buffer=samples.tobytes())
            self.sound_bank={
                'move':tone([(520,1)],.075,.17),
                'capture':tone([(360,.7),(540,.55)],.11,.22,'soft'),
                'check':tone([(660,.7),(880,.5)],.18,.2),
                'mate':tone([(392,.6),(494,.65),(587,.8),(784,.9),(988,.55)],.72,.18,'soft'),
                'win':tone([(523,.6),(659,.7),(784,.9),(1047,.8)],.48,.19),
                'loss':tone([(440,.8),(349,.7),(262,.9)],.48,.18),
                'draw':tone([(440,.7),(554,.7),(440,.7)],.35,.17),
                'click':tone([(700,1)],.035,.10),
            }
        except Exception:
            self.sound_bank={}
    def play_sound(self,name):
        try:
            snd=self.sound_bank.get(name)
            if snd: snd.play()
        except Exception: pass
    def text(self,s,x,y,size=20,color=TEXT,bold=False,center=False):
        # Rendering text every frame is expensive on some Windows/software-rendered setups.
        # Cache rendered surfaces and reuse them until the text/color/style changes.
        value=str(s); key=(value,size,tuple(color),bool(bold))
        surf=self.text_surfaces.get(key)
        if surf is None:
            surf=self.font(size,bold).render(value,True,color)
            if len(self.text_surfaces)>1800: self.text_surfaces.clear()
            self.text_surfaces[key]=surf
        rect=surf.get_rect()
        if center: rect.center=(int(x),int(y))
        else: rect.topleft=(int(x),int(y))
        self.screen.blit(surf,rect); return rect
    def panel(self,rect,color=PANEL,radius=14,border=None):
        pygame.draw.rect(self.screen,color,rect,border_radius=radius)
        if border: pygame.draw.rect(self.screen,border,rect,width=1,border_radius=radius)
    def button(self,rect,label,key,primary=False,small=False):
        hovered=rect.collidepoint(pygame.mouse.get_pos())
        color=(105,151,91) if primary and hovered else ACCENT_DARK if primary else (58,76,96) if hovered else PANEL2
        border=(158,207,137) if hovered else (101,132,91) if primary else (59,73,89)
        if hovered:
            glow=rect.inflate(8,8)
            pygame.draw.rect(self.screen,(42,64,52) if primary else (34,47,61),glow,border_radius=12)
            rect=rect.move(0,-1)
        pygame.draw.rect(self.screen,color,rect,border_radius=9)
        pygame.draw.rect(self.screen,border,rect,2 if hovered else 1,border_radius=9)
        self.text(label,rect.centerx,rect.centery,16 if small else 18,TEXT,True,True)
        self._active_buttons.append((rect,key))
    def notify(self,msg,seconds=4): self.notice=msg; self.notice_until=time.time()+seconds
    def _base(self):
        self.screen.fill(BG); w,h=self.screen.get_size()
        pygame.draw.rect(self.screen,(22,30,39),(0,0,w,74)); self.text('♟  CHESS',28,16,30,TEXT,True); self.text('OFFLINE  •  PLAY TOGETHER',30,49,11,ACCENT,True)
        if self.mode not in ('game','lan_game'):
            nick=self.font(16,True).render(self.nickname,True,MUTED)
            self.screen.blit(nick,nick.get_rect(topright=(w-24,28)))
        pygame.draw.line(self.screen,(48,61,75),(0,74),(w,74),1)
    def draw_home(self):
        self._base(); w,h=self.screen.get_size(); self._active_buttons=[]
        self.text('ТВОЯ ШАХМАТНАЯ АРЕНА',w//2,116,30,TEXT,True,True)
        self.text('Играй с ботом офлайн или подключайся к другу по локальной сети',w//2,151,15,MUTED,False,True)
        cardw=min(350,(w-100)//3); y=205; gap=20; total=cardw*3+gap*2; x0=(w-total)//2
        cards=[('♟','ТРЕНИРОВКА','Без рейтинга. Экспериментируй и учись.','train'),('♜','РЕЙТИНГ','Играй на результат против бота.','rated'),('♞','ИГРА ПО LAN','Создай комнату или введи IP друга.','lan')]
        for i,(ic,title,desc,key) in enumerate(cards):
            r=pygame.Rect(x0+i*(cardw+gap),y,cardw,245); self.panel(r,PANEL,16,(48,63,78))
            self.text(ic,r.centerx,y+55,42,ACCENT,True,True); self.text(title,r.centerx,y+108,19,TEXT,True,True)
            self.text(desc,r.centerx,y+145,14,MUTED,False,True)
            self.button(pygame.Rect(r.x+22,y+184,r.w-44,42),'Выбрать  →',key,key=='lan')
        r=pygame.Rect(x0,y+275,total,110); self.panel(r,PANEL,14)
        self.text('ПРОФИЛЬ',r.x+22,r.y+18,13,ACCENT,True); self.text(self.nickname,r.x+22,r.y+44,22,TEXT,True)
        if self.p.get('verified') or self.p.get('verified_before'):
            bx_badge=r.x+22+self.font(22,True).size(self.nickname)[0]+12
            pygame.draw.circle(self.screen,(55,139,237),(int(bx_badge+9),int(r.y+56)),10)
            pygame.draw.lines(self.screen,(255,255,255),False,[(bx_badge+4,r.y+56),(bx_badge+8,r.y+60),(bx_badge+15,r.y+51)],3)
        self.text(f"{self.p.get('elo',0)} ELO   •   Партий: {self.p.get('games',0)}   •   Побед: {self.p.get('wins',0)}",r.x+22,r.y+76,14,MUTED)
        self.button(pygame.Rect(r.right-190,r.y+31,165,42),'Профиль','profile')
        self.button(pygame.Rect(r.right-365,r.y+31,155,42),'Калькулятор','calculator')
        self.button(pygame.Rect(r.right-545,r.y+31,165,42),f'Бот: {self.level}/8','levels')
        if time.time()<self.notice_until: self.text(self.notice,w//2,h-25,14,SELECT,False,True)
    def draw_profile(self):
        self._base(); self._active_buttons=[]; w,h=self.screen.get_size()
        r=pygame.Rect(w//2-310,h//2-300,620,600); self.panel(r,PANEL,18,(54,76,96))
        self.text('ПРОФИЛЬ ИГРОКА',w//2,r.y+34,27,TEXT,True,True)
        # Minimal verification badges: blue queen before the nickname, purple check after it.
        nick_surf=self.font(25,True).render(self.nickname,True,TEXT)
        nick_rect=nick_surf.get_rect(center=(w//2,r.y+80))
        if self.p.get('verified'):
            qx=nick_rect.left-27; qy=nick_rect.centery
            pygame.draw.circle(self.screen,(55,139,237),(qx,qy),12)
            crown=[(qx-7,qy+5),(qx-8,qy-4),(qx-3,qy),(qx,qy-7),(qx+3,qy),(qx+8,qy-4),(qx+7,qy+5)]
            pygame.draw.polygon(self.screen,WHITE,crown)
            pygame.draw.line(self.screen,WHITE,(qx-6,qy+7),(qx+6,qy+7),2)
        self.screen.blit(nick_surf,nick_rect)
        if self.p.get('verified_before'):
            qx=nick_rect.right+27; qy=nick_rect.centery
            pygame.draw.circle(self.screen,(190,126,255),(qx,qy),12)
            pygame.draw.lines(self.screen,WHITE,False,[(qx-6,qy),(qx-2,qy+4),(qx+6,qy-5)],3)
            # Plain white attribution below the nickname: no card, border or outline.
            self.text('верифицировано создателем проекта',w//2,r.y+108,13,WHITE,False,True)
        self.text(f"Общий рейтинг: {int(self.p.get('elo',0))} ELO",w//2,r.y+137,16,MUTED,False,True)
        self.text('УРОВНИ БОТА — ELO ПО КАЖДОМУ УРОВНЮ',w//2,r.y+176,14,ACCENT,True,True)
        for i in range(8):
            col=i%4; row=i//4; xx=r.x+36+col*140; yy=r.y+204+row*55
            self.panel(pygame.Rect(xx,yy,128,45),PANEL2 if self.level!=i+1 else ACCENT_DARK,8)
            self.text(f'Уровень {i+1}',xx+64,yy+13,14,TEXT,True,True)
            self.text(f"{int(self.p.get('level_elo',{}).get(str(i+1),0))} ELO",xx+64,yy+31,11,MUTED,False,True)
            self._active_buttons.append((pygame.Rect(xx,yy,128,45),f'level:{i+1}'))
        self.button(pygame.Rect(r.centerx-130,r.bottom-56,260,40),'← Назад','home',True)
        self.button(pygame.Rect(r.centerx-130,r.bottom-104,260,36),'Выбрать уровень бота','levels',False,True)
        self.button(pygame.Rect(r.centerx-130,r.bottom-170,260,34),'Удаление данных','delete_data',False)
        if self.delete_confirm:
            # Confirmation modal blocks all controls behind it until Yes/No is chosen.
            w,h=self.screen.get_size()
            if self.delete_shade is None or self.delete_shade.get_size()!=(w,h):
                self.delete_shade=pygame.Surface((w,h),pygame.SRCALPHA)
                self.delete_shade.fill((3,7,12,190))
            self.screen.blit(self.delete_shade,(0,0))
            modal=pygame.Rect(0,0,460,220); modal.center=(w//2,h//2)
            self.panel(modal,(29,39,52),18,(87,105,123))
            self.text('УДАЛЕНИЕ ДАННЫХ',modal.centerx,modal.y+38,23,TEXT,True,True)
            self.text('Серьёзно удалить все данные профиля?',modal.centerx,modal.y+82,16,TEXT,False,True)
            self.text('Рейтинг, статистика и обе верификации будут сброшены.',modal.centerx,modal.y+110,12,MUTED,False,True)
            self._active_buttons=[]
            self.button(pygame.Rect(modal.x+24,modal.bottom-58,195,40),'Да, удалить','delete_data_yes',False)
            self.button(pygame.Rect(modal.right-219,modal.bottom-58,195,40),'Нет, отмена','delete_data_no',True)

    def draw_levels(self):
        self._base(); self._active_buttons=[]; w,h=self.screen.get_size()
        r=pygame.Rect(w//2-320,h//2-245,640,490); self.panel(r,PANEL,18,(54,76,96))
        self.text('ВЫБЕРИ УРОВЕНЬ БОТА',w//2,r.y+36,26,TEXT,True,True)
        self.text('Уровень 1 — самый лёгкий, уровень 8 — самый сильный',w//2,r.y+70,14,MUTED,False,True)
        for i in range(8):
            col=i%4; row=i//4; xx=r.x+35+col*145; yy=r.y+115+row*82
            rr=pygame.Rect(xx,yy,132,65)
            self.panel(rr,ACCENT_DARK if self.level==i+1 else PANEL2,10,(101,132,91) if self.level==i+1 else (59,73,89))
            self.text(f'Уровень {i+1}',rr.centerx,rr.y+23,15,TEXT,True,True)
            self.text(f"{int(self.p.get('level_elo',{}).get(str(i+1),0))} ELO",rr.centerx,rr.y+45,12,MUTED,False,True)
            self._active_buttons.append((rr,f'level:{i+1}'))
        if self.levels_for_mode:
            mode_label='ТРЕНИРОВКА' if self.levels_for_mode=='Тренировка' else 'РЕЙТИНГОВАЯ ПАРТИЯ'
            self.text(f'Режим: {mode_label}',w//2,r.bottom-92,13,ACCENT,True,True)
            self.button(pygame.Rect(r.centerx-245,r.bottom-58,235,40),'← Назад','levels_back',False)
            self.button(pygame.Rect(r.centerx+10,r.bottom-58,235,40),f'Играть · уровень {self.level}','start_selected',True)
        else:
            self.button(pygame.Rect(r.centerx-120,r.bottom-55,240,38),'← Назад','home',True)

    def draw_calculator(self):
        self._base(); self._active_buttons=[]; w,h=self.screen.get_size()
        r=pygame.Rect(w//2-230,h//2-250,460,500); self.panel(r,PANEL,18,(54,76,96))
        self.text('КАЛЬКУЛЯТОР',w//2,r.y+35,25,TEXT,True,True)
        box=pygame.Rect(r.x+24,r.y+65,r.w-48,54); self.panel(box,PANEL2,9,ACCENT)
        self.text(self.calc_expr or '0',box.right-14,box.y+14,22,TEXT,False,False)
        self.text(self.calc_result,r.centerx,r.y+130,15,GREEN if not self.calc_result.startswith('Ошибка') else RED,False,True)
        labels=('7','8','9','/','4','5','6','*','1','2','3','-','0','.','(', '+',')','%','C','⌫')
        bw,bh=88,43; gap=7; startx=r.centerx-(bw*4+gap*3)//2; starty=r.y+158
        for i,label in enumerate(labels):
            rr=pygame.Rect(startx+(i%4)*(bw+gap),starty+(i//4)*(bh+gap),bw,bh)
            self.button(rr,label,'calc:'+label, label=='=')
        self.button(pygame.Rect(r.x+24,r.bottom-58,190,40),'Посчитать / Enter','calc:=',True)
        self.button(pygame.Rect(r.x+230,r.bottom-58,206,40),'← Назад','home')
        self.text('Коды профиля вводятся здесь',w//2,r.bottom-12,11,MUTED,False,True)
    def calculate(self):
        value=self.calc_expr.strip()
        if value=='11111':
            self.p['verified']=True; save_profile(self.p); self.calc_result='✓ Профиль верифицирован'; self.notify('Красивая отметка верификации включена'); self.mode='home'; return
        if value=='22222':
            self.p['verified_before']=True; save_profile(self.p); self.calc_result='✓ Отметка подтверждена'; self.notify('Верификация профиля подтверждена'); self.mode='home'; return
        try:
            tree=ast.parse(value,mode='eval'); allowed={ast.Add:operator.add,ast.Sub:operator.sub,ast.Mult:operator.mul,ast.Div:operator.truediv,ast.FloorDiv:operator.floordiv,ast.Mod:operator.mod,ast.Pow:operator.pow,ast.UAdd:operator.pos,ast.USub:operator.neg}
            def ev(n):
                if isinstance(n,ast.Expression): return ev(n.body)
                if isinstance(n,ast.Constant) and isinstance(n.value,(int,float)) and abs(n.value)<10**12: return n.value
                if isinstance(n,ast.BinOp) and type(n.op) in allowed:
                    a,b=ev(n.left),ev(n.right)
                    if isinstance(n.op,ast.Pow) and abs(b)>8: raise ValueError()
                    result=allowed[type(n.op)](a,b)
                    if abs(result)>10**15: raise ValueError()
                    return result
                if isinstance(n,ast.UnaryOp) and type(n.op) in allowed: return allowed[type(n.op)](ev(n.operand))
                raise ValueError()
            ans=ev(tree); self.calc_result='= '+str(int(ans) if isinstance(ans,float) and ans.is_integer() else round(ans,6) if isinstance(ans,float) else ans)
        except Exception: self.calc_result='Ошибка выражения'

    def draw_profile_setup(self):
        self._base(); w,h=self.screen.get_size(); self._active_buttons=[]
        r=pygame.Rect(w//2-250,h//2-160,500,320); self.panel(r,PANEL,18)
        self.text('СОЗДАЙ ПРОФИЛЬ',w//2,r.y+48,26,TEXT,True,True); self.text('Ник игрока',r.x+42,r.y+100,15,MUTED)
        box=pygame.Rect(r.x+40,r.y+130,420,48); self.panel(box,PANEL2,8,ACCENT)
        self.text(self.overlay_input or self.nickname,box.x+12,box.y+13,18,TEXT)
        self.button(pygame.Rect(r.x+40,r.y+205,420,45),'Продолжить','save_nick',True)
        self.text('Введи ник с клавиатуры',w//2,r.y+275,13,MUTED,False,True)
    def draw_lan_setup(self):
        self._base(); w,h=self.screen.get_size(); self._active_buttons=[]
        r=pygame.Rect(w//2-285,h//2-210,570,420); self.panel(r,PANEL,18)
        self.text('ИГРА ПО ЛОКАЛЬНОЙ СЕТИ',w//2,r.y+42,25,TEXT,True,True)
        self.text('Хост запускает комнату на порту 50505.',w//2,r.y+82,14,MUTED,False,True)
        self.button(pygame.Rect(r.x+35,r.y+115,500,48),'Создать комнату (белые)','host',True)
        self.text('Или подключись к IP компьютера друга:',r.x+35,r.y+190,14,MUTED)
        inp=pygame.Rect(r.x+35,r.y+220,500,45); self.panel(inp,PANEL2,8,ACCENT if self.input_active else None)
        self.text(self.join_text,r.x+48,r.y+231,17,TEXT)
        self.button(pygame.Rect(r.x+35,r.y+280,500,45),'Подключиться (чёрные)','join',True)
        self.button(pygame.Rect(r.x+35,r.y+340,500,40),'Назад','home')
        self.text('Оба устройства должны быть в одной сети Wi-Fi/LAN.',w//2,r.y+395,12,MUTED,False,True)
    def animate_move(self, from_square, to_square, piece):
        """Start a short, timestamp-based animation; never sleeps the UI loop."""
        now = time.perf_counter()
        self.anim = {
            'from': from_square, 'to': to_square, 'piece': piece,
            'start': now, 'end': now + MOVE_ANIMATION_SECONDS,
        }

    def board_surface(self, size):
        """Cache the checkerboard layer so the 64 base squares are not redrawn every frame."""
        cached = self.board_surfaces.get(size)
        if cached is not None:
            return cached
        surf = pygame.Surface((size, size)).convert()
        cell = size / 8
        for row in range(8):
            for col in range(8):
                color = LIGHT if (row + col) % 2 == 0 else DARK
                rect = pygame.Rect(
                    round(col * cell), round(row * cell),
                    round((col + 1) * cell) - round(col * cell),
                    round((row + 1) * cell) - round(row * cell),
                )
                pygame.draw.rect(surf, color, rect)
        self.board_surfaces[size] = surf
        # A resize can create many sizes; keep the cache bounded.
        if len(self.board_surfaces) > 8:
            oldest = next(iter(self.board_surfaces))
            if oldest != size:
                self.board_surfaces.pop(oldest, None)
        return surf

    def draw_game(self):
        self._base(); self._active_buttons=[]; w,h=self.screen.get_size()
        margin=24; sidew=min(300,max(250,w//4)); board_size=min(h-145,w-sidew-90); board_size=max(320,board_size)
        board_size=min(board_size,h-125); bx=max(20,(w-sidew-board_size-55)//2); by=94
        self.board_rect=pygame.Rect(bx,by,board_size,board_size); cell=board_size/8
        lastset=set()
        if self.board.move_stack:
            lm=self.board.peek(); lastset={lm.from_square,lm.to_square}
        check_sq=self.board.king(self.board.turn) if self.board.is_check() else None
        self.screen.blit(self.board_surface(board_size), (bx, by))
        frame_now = time.perf_counter()
        anim_active = bool(self.anim and frame_now < self.anim['end'])
        for rr in range(8):
            for cc in range(8):
                sq=self.square_at(rr,cc); light=(rr+cc)%2==0
                col=None
                if sq in lastset: col=(195,211,126) if light else (145,174,83)
                if sq in self.targets: col=(192,220,171) if light else (87,137,92)
                if sq==self.selected: col=SELECT
                if sq==check_sq: col=(239,151,151) if light else (196,78,83)
                rect=pygame.Rect(round(bx+cc*cell),round(by+rr*cell),math.ceil(cell),math.ceil(cell))
                if col is not None:
                    pygame.draw.rect(self.screen,col,rect)
                piece=self.board.piece_at(sq)
                if anim_active and sq in (self.anim['from'],self.anim['to']): piece=None
                if sq in self.targets and not piece:
                    pygame.draw.circle(self.screen,(48,69,50),rect.center,max(4,int(cell*.10)))
                if piece:
                    sprite=self.piece_sprite(piece.symbol(),max(52,int(cell*.88)))
                    self.screen.blit(sprite,sprite.get_rect(center=rect.center))
                if sq==check_sq:
                    pygame.draw.rect(self.screen,RED,rect,width=4,border_radius=5)
        if self.anim:
            now=frame_now; t=min(1.0,max(0.0,(now-self.anim['start'])/(self.anim['end']-self.anim['start'])))
            if t>=1: self.anim=None
            else:
                a=self.anim; fr=a['from']; to=a['to']
                def center_of(sq):
                    rr=7-chess.square_rank(sq); cc=chess.square_file(sq)
                    if self.flipped: rr,cc=7-rr,7-cc
                    return (bx+(cc+.5)*cell,by+(rr+.5)*cell)
                x1,y1=center_of(fr); x2,y2=center_of(to)
                # Quintic smoothstep has zero velocity at both ends without lingering.
                ease=t*t*t*(t*(t*6-15)+10)
                cx=x1+(x2-x1)*ease; cy=y1+(y2-y1)*ease
                sprite=self.piece_sprite(a['piece'].symbol(),max(52,int(cell*.88)))
                self.screen.blit(sprite,sprite.get_rect(center=(round(cx),round(cy))))
        for i in range(8):
            fileidx=7-i if self.flipped else i; rankidx=i if self.flipped else 7-i
            self.text(chess.FILE_NAMES[fileidx],bx+i*cell+cell*.5,by+board_size+10,12,MUTED,False,True)
            self.text(str(8-rankidx),bx-12,by+i*cell+cell*.5,12,MUTED,False,True)
        sx=bx+board_size+30; sw=max(230,w-sx-22); sy=by
        panel=pygame.Rect(sx,sy,sw,h-sy-20); self.panel(panel,PANEL,14)
        title='LAN • Белые' if self.mode=='lan_game' and self.net_color else ('LAN • Чёрные' if self.mode=='lan_game' else self.game_mode.upper())
        self.text(title,sx+18,sy+18,18,TEXT,True)
        if self.mode=='lan_game':
            self.text('FastAPI • 50505',sx+18,sy+48,13,ACCENT,True)
            state='Соединено' if self.net_connected else 'Подключение…'
            self.text(state,sx+18,sy+73,13,GREEN if self.net_connected else SELECT)
        else:
            self.text(f'Уровень бота: {self.level}/8',sx+18,sy+50,14,MUTED)
        status=self.game_status(); self.text(status,sx+18,sy+102,14,RED if self.board.is_checkmate() else SELECT if 'шах' in status.lower() else TEXT,True)
        if self.board.is_checkmate():
            self.panel(pygame.Rect(sx+sw-82,sy+91,64,28),(111,35,45),8,(232,91,105)); self.text('МАТ',sx+sw-50,sy+105,13,(255,225,225),True,True)
        pygame.draw.line(self.screen,(54,67,82),(sx+16,sy+132),(sx+sw-16,sy+132),1)
        self.text('ХОДЫ',sx+18,sy+148,12,ACCENT,True)
        recent=list(self.board.move_stack)[-12:]
        for i,m in enumerate(recent):
            idx=len(self.board.move_stack)-len(recent)+i
            label=f'{idx//2+1}. ' if idx%2==0 else '     '
            self.text(label+m.uci(),sx+18,sy+176+i*23,14,TEXT if idx%2==0 else MUTED)
        btn_y=sy+min(485,panel.h-150)
        # Compact game panel: no hint/undo/flip/leave controls, only resignation.
        if not self.game_over():
            self.button(pygame.Rect(sx+16,panel.bottom-54,sw-32,38),'Сдаться','resign',False)
        # Menu stays in the header and is never clipped below the board.
        self.button(pygame.Rect(max(250,w-190),18,105,38),'← Меню','home',False,True)
        if self.result_overlay:
            self.draw_result_overlay()
        elif self.promotion_pending:
            self.draw_promotion_overlay()
        if time.time()<self.notice_until: self.text(self.notice,w//2,h-20,13,SELECT,False,True)

    def draw_promotion_overlay(self):
        """Draw a clear promotion picker above the board; it works in offline and LAN games."""
        w, h = self.screen.get_size()
        shade = pygame.Surface((w, h), pygame.SRCALPHA)
        shade.fill((5, 9, 14, 175))
        self.screen.blit(shade, (0, 0))
        card_w, card_h = 430, 192
        card = pygame.Rect(0, 0, min(card_w, w - 28), card_h)
        card.center = (w // 2, h // 2)
        self.panel(card, (27, 38, 51), 18, (86, 119, 143))
        self.text('ПРЕВРАЩЕНИЕ ПЕШКИ', card.centerx, card.y + 34, 20, TEXT, True, True)
        self.text('Выбери фигуру', card.centerx, card.y + 61, 13, MUTED, False, True)
        kinds = [('q', 'Ферзь'), ('r', 'Ладья'), ('b', 'Слон'), ('n', 'Конь')]
        bw, gap = 76, 15
        total = len(kinds) * bw + (len(kinds) - 1) * gap
        start_x = card.centerx - total // 2
        is_white = self.board.turn == chess.WHITE
        for i, (letter, label) in enumerate(kinds):
            rect = pygame.Rect(start_x + i * (bw + gap), card.y + 82, bw, 92)
            self.button(rect, '', f'promote:{letter}', False, True)
            symbol = letter.upper() if is_white else letter
            sprite = self.piece_sprite(symbol, 64)
            self.screen.blit(sprite, sprite.get_rect(center=(rect.centerx, rect.y + 37)))
            self.text(label, rect.centerx, rect.bottom - 13, 11, TEXT, True, True)

    def draw_result_overlay(self):
        w,h=self.screen.get_size(); now=time.time(); elapsed=now-self.result_overlay.get('started',now)
        if self.result_shade is None or self.result_shade.get_size()!=(w,h):
            self.result_shade=pygame.Surface((w,h),pygame.SRCALPHA); self.result_shade.fill((5,9,14,175))
        self.screen.blit(self.result_shade,(0,0))
        for particle in self.result_particles:
            age=elapsed-particle['born']
            if 0 <= age < particle['life']:
                px=int(particle['x']+particle['vx']*age+math.sin(age*4+particle['phase'])*12)
                py=int(particle['y']+particle['vy']*age)
                if -8<=px<=w+8 and -8<=py<=h+8:
                    rr=max(1,int(particle['size']*(1-age/particle['life'])))
                    pygame.draw.circle(self.screen,particle['color'],(px,py),rr)
        # Gentle scale-in without allocating expensive transformations each frame.
        t=min(1.0,max(0.0,elapsed/.32)); eased=1-(1-t)**3
        base_w=min(510,w-36); base_h=340; rw=int(base_w*(.92+.08*eased)); rh=int(base_h*(.92+.08*eased))
        r=pygame.Rect(0,0,rw,rh); r.center=(w//2,h//2+int((1-eased)*22))
        self.panel(r,(27,38,51),20,(78,101,122))
        won=self.result_overlay.get('won',False); drawn=self.result_overlay.get('draw',False)
        accent=(111,205,136) if won else ((239,195,93) if drawn else (236,112,120))
        # Decorative glow rings around the result emblem.
        pulse=.5+.5*math.sin(now*3.3)
        cx,cy=r.centerx,r.y+74
        glow=tuple(max(0,min(255,int(v*.28))) for v in accent)
        pygame.draw.circle(self.screen,glow,(cx,cy),int(44+5*pulse),2)
        pygame.draw.circle(self.screen,accent,(cx,cy),34,2)
        glyph='♕' if won else ('=' if drawn else '♔')
        self.text(glyph,cx,cy,43,TEXT,True,True)
        title=self.result_overlay.get('title','ПАРТИЯ ЗАВЕРШЕНА')
        self.text(title,cx,r.y+130,25,TEXT,True,True)
        self.text(self.result_overlay.get('subtitle',''),cx,r.y+166,15,MUTED,False,True)
        self.text(self.result_overlay.get('elo',''),cx,r.y+204,22,accent,True,True)
        self.text(self.result_overlay.get('detail',''),cx,r.y+235,13,MUTED,False,True)
        by=r.bottom-58
        if self.mode=='lan_game':
            self.button(pygame.Rect(r.centerx-125,by,250,40),'В меню','result_home',True)
        else:
            bw=(r.w-58)//2
            self.button(pygame.Rect(r.x+20,by,bw,40),'Сыграть ещё','result_again',True)
            self.button(pygame.Rect(r.x+38+bw,by,bw,40),'В меню','result_home',False)
    def square_at(self,r,c):
        if self.flipped: r,c=7-r,7-c
        return chess.square(c,7-r)
    def game_status(self):
        if self.game_result_override: return 'Сдача — партия завершена' if self.game_result_override in ('0-1','1-0') else 'Партия завершена'
        if self.board.is_checkmate(): return 'МАТ — партия завершена'
        if self.board.is_stalemate() or self.board.is_insufficient_material(): return 'Ничья'
        if self.board.is_check(): return 'Шах!'
        if self.mode=='lan_game':
            if not self.net_connected: return 'Ожидание соперника…'
            return 'Твой ход' if self.board.turn==self.net_color else 'Ход соперника…'
        if self.thinking: return 'Бот думает…'
        return 'Твой ход (белые)' if self.board.turn else 'Ход бота (чёрные)'
    def draw(self):
        if self.mode=='home': self.draw_home()
        elif self.mode=='profile': self.draw_profile()
        elif self.mode=='levels': self.draw_levels()
        elif self.mode=='calculator': self.draw_calculator()
        elif self.mode=='profile_setup': self.draw_profile_setup()
        elif self.mode=='lan_setup': self.draw_lan_setup()
        elif self.mode in ('game','lan_game'): self.draw_game()
        pygame.display.flip()
    def poll_bot_process(self):
        future=self.bot_future
        if future is None or not future.done() or time.monotonic() < self.bot_due:
            return
        self.bot_future=None
        try:
            result=future.result()
        except Exception as exc:
            self.thinking=False
            self.notify(f'Ошибка движка бота: {exc}', 6)
            return
        self.net_queue.put(('bot', result))

    def game_over(self):
        return bool(self.game_result_override) or self.board.is_game_over(claim_draw=True)

    def start_game(self,mode):
        self._stop_network()
        if self.bot_future is not None:
            self.bot_future.cancel(); self.bot_future=None
        self.pending_bot_request=None; self.pending_bot_after=0.0
        self.thinking=False
        self.game_result_override=''; self.game_finished=False; self.result_overlay=None; self.thinking=False; self.promotion_pending=None; self.resigned=False; self.game_mode=mode; self.mode='game'; self.board=chess.Board(); self.selected=None; self.targets=[]
        self.p['mode']=mode; self.p['level']=self.level; self.rating_game=mode=='Рейтинг'; self.p['rated_active']=self.rating_game; self.persist()
    def persist(self):
        if self.mode=='lan_game': return
        self.p['fen']=self.board.fen(); self.p['mode']=self.game_mode; self.p['level']=self.level; self.p['rated_active']=self.rating_game; save_profile(self.p)
    def start_host(self):
        global LAN_BOARD,LAN_PLAYERS,LAN_LAST,LAN_REV,LAN_STARTED,LAN_RESULT
        try:
            probe=socket.socket(); probe.bind(('0.0.0.0',50505)); probe.close()
        except OSError as e: self.notify(f'Порт 50505 занят: {e}'); return
        with LAN_LOCK: LAN_BOARD=chess.Board(); LAN_PLAYERS=1; LAN_LAST=''; LAN_RESULT=''; LAN_REV+=1; LAN_STARTED=False
        # PyInstaller --windowed/--noconsole sets sys.stderr (and sometimes
        # sys.stdout) to None. Uvicorn's default logging formatter calls
        # .isatty() on that stream and crashes before the LAN server starts.
        # Disabling Uvicorn's default logging configuration avoids that path.
        try:
            config=uvicorn.Config(
                api, host='0.0.0.0', port=50505,
                log_level='error', access_log=False, log_config=None,
                use_colors=False,
            )
            self.server=uvicorn.Server(config)
            self.server_thread=threading.Thread(target=self.server.run, daemon=True)
            self.server_thread.start()
        except Exception as e:
            self.notify(f'Не удалось запустить LAN-сервер: {e}', 8)
            return
        self.game_result_override=''; self.net_url='http://127.0.0.1:50505'; self.net_role='host'; self.net_color=chess.WHITE; self.net_connected=False; self.net_revision=-1
        self.mode='lan_game'; self.board=chess.Board(); self._start_poller()
        ip=self.local_ip(); self.notify(f'Комната создана. IP для друга: {ip}:50505',10)
    def local_ip(self):
        s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        try: s.connect(('192.0.2.1',80)); return s.getsockname()[0]
        except OSError: return '127.0.0.1'
        finally: s.close()
    def join_host(self):
        host=self.join_text.strip()
        if not host: self.notify('Введи IP компьютера хоста'); return
        if '://' not in host: host='http://'+host
        host=host.rstrip('/')
        try:
            r=requests.post(host+'/join',timeout=2)
            if r.status_code==409: self.notify('Комната заполнена'); return
            r.raise_for_status()
        except Exception as e: self.notify(f'Не удалось подключиться: {e}',7); return
        self._stop_network(); self.game_result_override=''; self.net_url=host; self.net_role='guest'; self.net_color=chess.BLACK; self.net_connected=True; self.net_revision=-1
        self.mode='lan_game'; self.board=chess.Board(); self._start_poller(); self.notify('Подключено! Ты играешь чёрными.')
    def _start_poller(self):
        self.net_stop.clear()
        def poll():
            while not self.net_stop.is_set():
                try:
                    r=requests.get(self.net_url+'/state',timeout=1.5); r.raise_for_status(); self.net_queue.put(('state',r.json()))
                except Exception as e: self.net_queue.put(('error',str(e)))
                self.net_stop.wait(.35)
        threading.Thread(target=poll,daemon=True).start()
    def _stop_network(self):
        self.net_stop.set()
        if self.net_role=='host' and self.server:
            try: self.server.should_exit=True
            except Exception: pass
        self.server=None; self.server_thread=None; self.net_role=''; self.net_url=''; self.net_connected=False
    def process_network(self):
        while True:
            try: kind,data=self.net_queue.get_nowait()
            except Empty: break
            if kind=='bot':
                if self.mode=='game':
                    self.thinking=False
                    if self.game_over(): continue
                    try: m=chess.Move.from_uci(data) if data else None
                    except ValueError: m=None
                    if m and m in self.board.legal_moves:
                        was_capture=self.board.is_capture(m); piece=self.board.piece_at(m.from_square); self.board.push(m); self.play_sound('capture' if was_capture else 'move'); self.animate_move(m.from_square,m.to_square,piece or chess.Piece(chess.QUEEN,chess.BLACK)); self.persist()
                    if self.board.is_check() and not self.game_over(): self.play_sound('check')
                    if self.game_over(): self.finish_game()
                continue
            if self.mode!='lan_game': continue
            if kind=='error':
                if self.net_connected: self.notify('Потеряно соединение с сервером',5)
                self.net_connected=False; continue
            self.net_connected=data.get('players',0)>=2 and data.get('started',False)
            try:
                rev=data['revision']
                if rev!=self.net_revision:
                    old_board=self.board.copy()
                    # Rebuild from the authoritative move list, not FEN alone: FEN loses history.
                    new_board=chess.Board()
                    for uci in data.get('moves', []):
                        new_board.push_uci(uci)
                    if new_board.move_stack:
                        lm=new_board.peek(); piece=old_board.piece_at(lm.from_square)
                        if piece: self.animate_move(lm.from_square,lm.to_square,piece)
                    self.board=new_board; self.net_revision=rev
                    self.selected=None; self.targets=[]
                    if self.board.move_stack: self.last_move=self.board.peek()
                    # A checkmate/stalemate can be derived from the move list itself.
                    # Only keep an override for external endings such as resignation.
                    if data.get('game_over') and data.get('result') and not self.board.is_game_over(claim_draw=True):
                        self.game_result_override=data.get('result','')
                    if self.game_over():
                        result=self.game_result_override or self.board.result(claim_draw=True)
                        if self.result_overlay is None:
                            white_won=(result=='1-0')
                            drawn=result=='1/2-1/2'
                            local_won=(white_won==bool(self.net_color)) and not drawn
                            title='НИЧЬЯ' if drawn else ('ПОБЕДА!' if local_won else 'ПОРАЖЕНИЕ')
                            self.result_overlay={'started':time.time(),'won':local_won,'draw':drawn,'title':title,
                                'subtitle':'LAN • матч завершён','elo':'ELO не меняется',
                                'detail':'Результат получен от сервера FastAPI'}
                            self.play_sound('draw' if drawn else 'win' if local_won else 'loss')
            except (ValueError,KeyError): pass
    def send_move(self,move):
        try:
            r=requests.post(self.net_url+'/move',json={'uci':move.uci(),'player':self.net_role},timeout=1.5)
            if not r.ok: self.notify(r.json().get('detail','Ход не принят сервером'))
            else: self.process_network()
        except Exception as e: self.notify(f'Ошибка отправки хода: {e}')
    def handle_board_click(self,pos):
        # While the promotion picker is open, ignore clicks on the board.
        if self.promotion_pending is not None:
            return
        if not self.board_rect.collidepoint(pos) or self.thinking or self.game_over(): return
        if self.mode=='lan_game' and (not self.net_connected or self.board.turn!=self.net_color): return
        if self.mode=='game' and not self.board.turn: return
        cell=self.board_rect.w/8; c=int((pos[0]-self.board_rect.x)/cell); r=int((pos[1]-self.board_rect.y)/cell); sq=self.square_at(r,c); piece=self.board.piece_at(sq)
        own=self.net_color if self.mode=='lan_game' else chess.WHITE
        if self.selected is None:
            if piece and piece.color==own:
                self.selected=sq; self.targets=[m.to_square for m in self.board.legal_moves if m.from_square==sq]
            return

        # Resolve the destination against all legal moves. Promotion moves are special:
        # chess libraries represent e7-e8=Q as a distinct move, not plain e7-e8.
        choices=[m for m in self.board.legal_moves if m.from_square==self.selected and m.to_square==sq]
        if not choices:
            if piece and piece.color==own:
                self.selected=sq; self.targets=[m.to_square for m in self.board.legal_moves if m.from_square==sq]
            else:
                self.selected=None; self.targets=[]
            return
        promotions=[m for m in choices if m.promotion]
        if promotions:
            self.promotion_pending=(self.selected,sq)
            return
        self.commit_human_move(choices[0])

    def commit_human_move(self,move):
        """Apply a chosen legal human move, including promotion, offline or over LAN."""
        if move not in self.board.legal_moves:
            self.notify('Недопустимый ход')
            self.selected=None; self.targets=[]
            return
        if self.mode=='lan_game':
            # The server is authoritative. Send the full UCI move, including a promotion suffix.
            self.send_move(move)
            self.selected=None; self.targets=[]
            return

        was_capture=self.board.is_capture(move)
        piece=self.board.piece_at(move.from_square)
        self.board.push(move)
        self.play_sound('capture' if was_capture else 'move')
        self.animate_move(move.from_square,move.to_square,piece or chess.Piece(chess.QUEEN,chess.WHITE))
        self.selected=None; self.targets=[]; self.promotion_pending=None; self.persist()
        if self.board.is_check() and not self.game_over(): self.play_sound('check')
        if self.game_over():
            self.finish_game()
            return
        self.thinking=True; level=self.level
        # Start bot work only after the short movement animation, so rendering stays responsive.
        if self.bot_future is not None and not self.bot_future.done():
            self.bot_future.cancel()
        self.pending_bot_request=(self.board.fen(), level)
        self.pending_bot_after=self.anim['end'] if self.anim else time.perf_counter()

    def finish_game(self):
        if self.mode!='game' or self.game_finished: return
        self.game_finished=True; self.thinking=False
        self.p['games']=int(self.p.get('games',0))+1
        result=self.game_result_override or self.board.result(claim_draw=True)
        was_rating=bool(self.rating_game)
        if result=='1-0':
            self.p['wins']=int(self.p.get('wins',0))+1; delta=20
            title='ПОБЕДА!'; won=True; drawn=False
        elif result=='0-1':
            self.p['losses']=int(self.p.get('losses',0))+1; delta=-10
            title='ВЫ СДАЛИСЬ' if self.resigned else 'ПОРАЖЕНИЕ'; won=False; drawn=False
        else:
            self.p['draws']=int(self.p.get('draws',0))+1; delta=0
            title='НИЧЬЯ'; won=False; drawn=True

        # Training is practice: never deduct ELO for a loss or resignation.
        # Rating and per-level ELO only change in explicitly rated games.
        level_elo=self.p.setdefault('level_elo',{str(i):0 for i in range(1,9)})
        level_elo.setdefault(str(self.level),0)
        if was_rating:
            old_level=int(level_elo.get(str(self.level),0))
            level_delta=(20 if won else (-10 if not drawn else 0))
            level_elo[str(self.level)]=max(0,old_level+level_delta)
            self.p['elo']=max(0,int(self.p.get('elo',0))+delta)
            displayed_delta=delta
        else:
            displayed_delta=0

        self.rating_game=False; self.p['rated_active']=False; self.p['fen']=''; save_profile(self.p)
        mode_name='Рейтинговая партия' if self.game_mode=='Рейтинг' else 'Тренировка'
        result_time=time.time()
        self.result_particles=[]
        if won:
            confetti_colors=[(111,205,136),(91,164,255),(242,201,109),(205,140,255),(255,242,220)]
            for _ in range(72):
                self.result_particles.append({'x':random.randint(0, max(1,self.screen.get_width())), 'y':random.randint(-self.screen.get_height(),0), 'vx':random.uniform(-28,28), 'vy':random.uniform(45,145), 'size':random.randint(2,5), 'life':random.uniform(2.5,4.5), 'born':0.0, 'phase':random.random()*math.tau, 'color':random.choice(confetti_colors)})
        if self.resigned:
            subtitle='Ты сдался • партия завершена'
            elo_text='Рейтинг не изменён' if not was_rating else f'{displayed_delta:+d} ELO'
        else:
            subtitle=f'{mode_name} • уровень бота {self.level}/8'
            elo_text=f'{displayed_delta:+d} ELO' if displayed_delta else 'ELO без изменений'
        self.result_overlay={
            'started':result_time, 'won':won, 'draw':drawn, 'title':title,
            'subtitle':subtitle,
            'elo':elo_text,
            'detail':f'Рейтинг уровня: {int(level_elo[str(self.level)])} ELO  •  Всего партий: {self.p.get("games",0)}'
        }
        if self.board.is_checkmate(): self.play_sound('mate')
        else: self.play_sound('win' if won else 'draw' if drawn else 'loss')

    def click_button(self,key):
        self.play_sound('click')
        if key=='delete_data':
            self.delete_confirm=True; return
        if key=='delete_data_no':
            self.delete_confirm=False; return
        if key=='delete_data_yes':
            # Remove persisted information and reset the in-memory profile to clean defaults.
            try: SAVE.unlink(missing_ok=True)
            except OSError: pass
            self.p=fresh_profile(); self.nickname=self.p['nickname']; self.level=int(self.p['level'])
            self.rating_game=False; self.game_result_override=''; self.game_finished=False
            self.resigned=False; self.calc_expr=''; self.calc_result=''; self.delete_confirm=False
            self.selected=None; self.targets=[]; self.promotion_pending=None; self.result_overlay=None
            self.mode='profile'; save_profile(self.p)
            self.notify('Все данные профиля удалены',5); return
        if key.startswith('promote:'):
            if not self.promotion_pending:
                return
            letter = key.split(':', 1)[1].lower()
            promotion_type = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}.get(letter)
            from_sq, to_sq = self.promotion_pending
            self.promotion_pending = None
            move = chess.Move(from_sq, to_sq, promotion=promotion_type) if promotion_type else None
            if move and move in self.board.legal_moves:
                self.commit_human_move(move)
            else:
                self.notify('Это превращение сейчас недоступно')
            return
        if key=='result_home':
            if self.mode=='lan_game': self._stop_network()
            self.result_overlay=None; self.mode='home'; return
        if key=='result_again':
            self.result_overlay=None; self.start_game(self.game_mode); return
        if key=='calculator': self.mode='calculator'; self.calc_expr=''; self.calc_result=''; return
        if key.startswith('calc:'):
            ch=key.split(':',1)[1]
            if ch=='=': self.calculate()
            elif ch=='C': self.calc_expr=''; self.calc_result=''
            elif ch=='⌫': self.calc_expr=self.calc_expr[:-1]
            else: self.calc_expr+=ch
            return
        if key=='resign':
            if self.mode=='game':
                self.resigned=True; self.game_result_override='0-1'; self.finish_game()
            elif self.mode=='lan_game':
                try:
                    rr=requests.post(self.net_url+'/resign',json={'player':self.net_role},timeout=1.5); rr.raise_for_status(); self.notify('Ты сдался. Партия завершена.',8)
                except Exception as e: self.notify(f'Не удалось сдаться: {e}')
            return
        if key=='home':
            if self.mode=='lan_game': self._stop_network()
            self.pending_bot_request=None; self.promotion_pending=None
            self.mode='home'; self.thinking=False; self.overlay_input=''; self.levels_for_mode=None; return
        if key=='train': self.levels_for_mode='Тренировка'; self.mode='levels'
        elif key=='rated': self.levels_for_mode='Рейтинг'; self.mode='levels'
        elif key=='start_selected':
            target=self.levels_for_mode or 'Тренировка'; self.levels_for_mode=None; self.start_game(target)
        elif key=='levels_back': self.levels_for_mode=None; self.mode='home'
        elif key=='lan': self.mode='lan_setup'; self.join_text='127.0.0.1'
        elif key=='host': self.start_host()
        elif key=='join': self.join_host()
        elif key=='profile': self.mode='profile'
        elif key=='profile_nick': self.mode='profile_setup'; self.overlay_input=self.nickname
        elif key.startswith('level:'):
            self.level=max(1,min(8,int(key.split(':')[1]))); self.p['level']=self.level; save_profile(self.p)
            if self.levels_for_mode:
                self.notify(f'Выбран уровень {self.level}/8 — нажми «Играть»')
            else:
                self.mode='home'; self.notify(f'Выбран уровень бота {self.level}/8')
        elif key=='levels': self.mode='levels'
        elif key=='save_nick':
            name=self.overlay_input.strip()
            if name: self.nickname=name[:20]; self.p['nickname']=self.nickname; save_profile(self.p); self.mode='home'
            else: self.notify('Ник не может быть пустым')
        elif key=='new': self.start_game(self.game_mode)
        elif key=='flip': self.flipped=not self.flipped
        elif key=='undo' and len(self.board.move_stack)>=2:
            self.board.pop(); self.board.pop(); self.persist()
        elif key=='hint':
            try: _,m=search(self.board.copy(),2,-10**9,10**9,time.monotonic()+.5)
            except TimeoutError: m=None
            if m: self.notify(f'Подсказка: {m.uci()}')
        elif key=='leave': self._stop_network(); self.mode='home'
    def update_cursor(self):
        # set_cursor may cause expensive platform calls on Windows if repeated every frame.
        # Only update the OS cursor when entering or leaving a clickable control.
        pos=pygame.mouse.get_pos()
        is_button=any(rect.collidepoint(pos) for rect,key in self._active_buttons)
        if is_button == self._cursor_is_hand: return
        try:
            pygame.mouse.set_cursor(pygame.SYSTEM_CURSOR_HAND if is_button else pygame.SYSTEM_CURSOR_ARROW)
            self._cursor_is_hand=is_button
        except pygame.error: pass

    def run(self):
        while self.running:
            # Keep the buttons drawn in the previous frame until mouse events are handled.
            # Each draw_* method rebuilds this list after event processing.
            for event in pygame.event.get():
                if event.type==pygame.QUIT: self.running=False
                elif event.type==pygame.VIDEORESIZE:
                    self.screen=pygame.display.set_mode((max(900,event.w),max(620,event.h)),pygame.RESIZABLE)
                elif event.type==pygame.MOUSEBUTTONDOWN and event.button==1:
                    handled=False
                    for rect,key in reversed(self._active_buttons):
                        if rect.collidepoint(event.pos): self.click_button(key); handled=True; break
                    if not handled and self.mode in ('game','lan_game') and not self.result_overlay: self.handle_board_click(event.pos)
                    elif not handled and self.mode=='lan_setup':
                        w,h=self.screen.get_size(); r=pygame.Rect(w//2-250,h//2+10,500,45); self.input_active=r.collidepoint(event.pos)
                elif event.type==pygame.KEYDOWN:
                    if self.mode=='profile_setup':
                        if event.key==pygame.K_RETURN: self.click_button('save_nick')
                        elif event.key==pygame.K_BACKSPACE: self.overlay_input=self.overlay_input[:-1]
                        elif event.unicode.isprintable() and len(self.overlay_input)<20: self.overlay_input+=event.unicode
                    elif self.mode=='calculator':
                        if event.key==pygame.K_RETURN: self.calculate()
                        elif event.key==pygame.K_BACKSPACE: self.calc_expr=self.calc_expr[:-1]
                        elif event.unicode in '0123456789+-*/().% ': self.calc_expr+=event.unicode
                    elif self.mode=='lan_setup' and self.input_active:
                        if event.key==pygame.K_RETURN: self.click_button('join')
                        elif event.key==pygame.K_BACKSPACE: self.join_text=self.join_text[:-1]
                        elif event.unicode.isprintable() and len(self.join_text)<100: self.join_text+=event.unicode
                    elif event.key==pygame.K_ESCAPE:
                        if self.delete_confirm: self.delete_confirm=False
                        elif self.mode in ('game','lan_game'): self.click_button('home')
                        elif self.mode!='home': self.mode='home'
            if (self.mode == 'game' and self.pending_bot_request is not None and self.bot_future is None
                    and time.perf_counter() >= self.pending_bot_after):
                fen, bot_level = self.pending_bot_request
                self.pending_bot_request = None
                try:
                    self.bot_future = self.bot_executor.submit(bot_move_uci_from_fen, fen, bot_level)
                    self.bot_due = time.monotonic() + max(0.08, {1:.12, 2:.14, 3:.15, 4:.16, 5:.17, 6:.18, 7:.20, 8:.22}.get(bot_level, .14))
                except Exception as exc:
                    self.thinking=False
                    self.notify(f'Не удалось запустить движок бота: {exc}', 6)
            self.poll_bot_process()
            self.process_network()
            while True:
                try: kind,data=self.net_queue.get_nowait()
                except Empty: break
                if kind=='bot' and self.mode=='game':
                    self.thinking=False
                    try: m=chess.Move.from_uci(data) if data else None
                    except ValueError: m=None
                    if m and m in self.board.legal_moves:
                        piece=self.board.piece_at(m.from_square); self.board.push(m); self.animate_move(m.from_square,m.to_square,piece or chess.Piece(chess.QUEEN,chess.BLACK)); self.persist()
                    if self.game_over(): self.finish_game()
            self.draw(); self.update_cursor(); self.clock.tick(60)
        self._stop_network(); save_profile(self.p)
        try: self.bot_executor.shutdown(wait=False, cancel_futures=True)
        except Exception: pass
        pygame.quit()

def main(): ChessApp().run()
if __name__=='__main__':
    multiprocessing.freeze_support()  # required for ProcessPoolExecutor in a Windows EXE
    main()

"""Native Telegram group, poll and privacy operations. No separate account client."""
import copy
import functools
import re
from urllib.parse import urlparse
from telethon import functions as F, types as T, utils
from telethon.errors import FloodWaitError


PRIVACY = {
    'last_seen': T.InputPrivacyKeyStatusTimestamp,
    'phone': T.InputPrivacyKeyPhoneNumber,
    'profile_photo': T.InputPrivacyKeyProfilePhoto,
    'group_invites': T.InputPrivacyKeyChatInvite,
    'calls': T.InputPrivacyKeyPhoneCall,
    'forwards': T.InputPrivacyKeyForwards,
}
STRING = {'type': 'string'}
BOOL = {'type': 'boolean'}
CHAT = {'chat': STRING}


def nonempty(value, maximum):
    if not isinstance(value, str) or not value.strip() or len(value)>maximum:
        raise ValueError('invalid_text')
    return value.strip()


def register(tool, api):
    def expose(name, description, properties, required=(), mutation=False):
        def decorate(fn):
            @functools.wraps(fn)
            async def wrapped(args):
                if not isinstance(args,dict) or any(key not in args for key in required):
                    raise ValueError('missing_required_arguments')
                for key,value in args.items():
                    spec=properties.get(key)
                    if spec is None:raise ValueError('unknown_argument: '+key)
                    typ=spec.get('type')
                    if typ=='string' and not isinstance(value,str) or typ=='boolean' and type(value) is not bool or typ=='integer' and type(value) is not int or typ=='array' and not isinstance(value,list):raise ValueError('invalid_argument: '+key)
                    if 'enum' in spec and value not in spec['enum']:raise ValueError('invalid_argument: '+key)
                api['need_auth']()
                try:
                    if mutation:
                        async with api['S'].send_lock:
                            await api['sync_guard']()
                            return await fn(args)
                    await api['sync_guard']()
                    return await fn(args)
                except FloodWaitError as error:
                    api['set_flood'](error.seconds,name)
                    api['record_sync_error'](None,error)
                    raise
            return tool(name,description,{'type':'object','properties':properties,'required':list(required),'additionalProperties':False})(wrapped)
        return decorate

    async def peer(args, group=False, monitored=False):
        cid=await api['resolve'](args['chat'])
        if monitored:api['require_monitoring'](cid)
        await api['sync_guard']()
        entity=await api['S'].client.get_entity(cid)
        if group and not (isinstance(entity,T.Chat) or isinstance(entity,T.Channel) and entity.megagroup):
            raise ValueError('group_required')
        return cid,entity

    async def mutate(cid, call):
        await api['sync_guard']()
        if not api['health_ok']():raise RuntimeError('platform_cooldown')
        reason=api['check_send_limits'](cid)
        if reason:raise RuntimeError('rate_limit: '+reason)
        reservation=api['record_send'](cid)
        try:return await call()
        except FloodWaitError:
            api['release_rejected_send'](reservation)
            raise

    async def rpc(cid, request):
        return await mutate(cid,lambda:api['S'].client(request))

    @expose('list_groups','Lista los grupos conocidos, sin descargar participantes.',{'limit':{'type':'integer'},'offset':{'type':'integer'}})
    async def groups(a):
        limit=max(1,min(a.get('limit',50),100));offset=max(0,a.get('offset',0))
        c=api['db']()
        try:return {'groups':[dict(r) for r in c.execute("SELECT id,title,type FROM chats WHERE type IN ('group','supergroup') ORDER BY id LIMIT ? OFFSET ?",(limit,offset))],'next_offset':offset+limit}
        finally:c.close()

    @expose('get_group_info','Información de un grupo monitoreado; hasta 200 participantes.',CHAT,('chat',))
    async def info(a):
        await peer(a,group=True,monitored=True)
        return await api['t_info'](a)

    @expose('create_group','Crea un grupo con 1 a 10 participantes expresamente solicitados.',{'name':STRING,'participants':{'type':'array','items':STRING}},('name','participants'),True)
    async def create(a):
        name=nonempty(a['name'],128);members=a['participants']
        if not 1<=len(members)<=10 or len(set(members))!=len(members):raise ValueError('participants_limit_1_to_10')
        users=[]
        for member in members:
            await api['sync_guard']()
            users.append(utils.get_input_user(await api['S'].client.get_input_entity(nonempty(member,128))))
        result=await rpc(0,F.messages.CreateChatRequest(users=users,title=name))
        return {'ok':True,'groups':[{'id':utils.get_peer_id(c),'title':c.title} for c in getattr(result,'chats',getattr(getattr(result,'updates',None),'chats',[]))]}

    @expose('leave_group','Sale del grupo indicado.',CHAT,('chat',),True)
    async def leave(a):
        cid,ent=await peer(a,True)
        request=F.channels.LeaveChannelRequest(ent) if isinstance(ent,T.Channel) else F.messages.DeleteChatUserRequest(ent.id,T.InputUserSelf())
        await rpc(cid,request);return {'ok':True}

    @expose('update_group_participants','Añade, retira, promueve o degrada hasta 10 participantes. Se detiene ante el primer límite/error y devuelve progreso parcial; no repetir los miembros ya actualizados.',dict(CHAT,participants={'type':'array','items':STRING},action={'type':'string','enum':['add','remove','promote','demote']}),('chat','participants','action'),True)
    async def participants(a):
        members=a['participants'];action=a['action']
        if not 1<=len(members)<=10 or any(not isinstance(x,str) or not x.strip() for x in members) or len(set(members))!=len(members):raise ValueError('participants_limit_1_to_10')
        cid,ent=await peer(a,True);done=[]
        for index,member in enumerate(members):
            attempted=False
            async def apply_change():
                nonlocal attempted
                attempted=True
                if action=='add':
                    req=F.channels.InviteToChannelRequest(ent,[user]) if isinstance(ent,T.Channel) else F.messages.AddChatUserRequest(ent.id,user,0)
                    return await api['S'].client(req)
                if action=='remove':return await api['S'].client.kick_participant(ent,user)
                return await api['S'].client.edit_admin(ent,user,is_admin=action=='promote')
            try:
                await api['sync_guard']()
                user=await api['S'].client.get_input_entity(member)
                await mutate(cid,apply_change)
            except Exception as error:
                if isinstance(error,FloodWaitError):
                    api['set_flood'](error.seconds,'update_group_participants')
                    api['record_sync_error'](None,error)
                return {'ok':False,'updated':done,'failed_member':member,
                        'failed_outcome':'unknown' if attempted else 'not_attempted',
                        'remaining':members[index+1:],'error':str(error),
                        'error_type':type(error).__name__}
            done.append(member)
        return {'ok':True,'updated':done}

    @expose('set_group_name','Cambia el nombre del grupo.',dict(CHAT,name=STRING),('chat','name'),True)
    async def name(a):
        value=nonempty(a['name'],128);cid,ent=await peer(a,True)
        req=F.channels.EditTitleRequest(ent,value) if isinstance(ent,T.Channel) else F.messages.EditChatTitleRequest(ent.id,value)
        await rpc(cid,req);return {'ok':True}

    @expose('set_group_topic','Cambia la descripción del grupo.',dict(CHAT,topic=STRING),('chat','topic'),True)
    async def topic(a):
        if len(a['topic'])>255:raise ValueError('description_too_long')
        cid,ent=await peer(a,True)
        await rpc(cid,F.messages.EditChatAboutRequest(ent,a['topic']));return {'ok':True}

    async def permissions(a,flag,value):
        cid,ent=await peer(a,True)
        rights=copy.copy(getattr(ent,'default_banned_rights',None) or T.ChatBannedRights(None))
        setattr(rights,flag,value)
        await rpc(cid,F.messages.EditChatDefaultBannedRightsRequest(ent,rights));return {'ok':True}

    @expose('set_group_announce','Restringe el envío del grupo a administradores.',dict(CHAT,announce_only=BOOL),('chat','announce_only'),True)
    async def announce(a):return await permissions(a,'send_messages',a['announce_only'])

    @expose('set_group_locked','Restringe cambios de información a administradores, conservando otros permisos.',dict(CHAT,locked=BOOL),('chat','locked'),True)
    async def locked(a):return await permissions(a,'change_info',a['locked'])

    @expose('get_group_invite_link','Exporta un enlace del grupo; reset revoca el enlace permanente anterior.',dict(CHAT,reset=BOOL),('chat',),True)
    async def invite(a):
        cid,ent=await peer(a,True)
        result=await rpc(cid,F.messages.ExportChatInviteRequest(ent,legacy_revoke_permanent=bool(a.get('reset'))))
        return {'link':result.link}

    @expose('join_group_with_link','Se une a un grupo mediante un enlace privado t.me/+ o t.me/joinchat/.',{'link':STRING},('link',),True)
    async def join(a):
        url=urlparse(a['link'])
        if url.scheme!='https' or url.netloc!='t.me' or url.query or url.fragment:raise ValueError('invalid_invite_link')
        match=re.fullmatch(r'/(?:\+|joinchat/)([A-Za-z0-9_-]+)',url.path)
        if not match:raise ValueError('invalid_invite_link')
        await rpc(0,F.messages.ImportChatInviteRequest(match[1]));return {'ok':True}

    @expose('send_poll','Envía una encuesta; 2 a 10 opciones. Telegram admite selección única o múltiple, sin máximo intermedio.',dict(CHAT,question=STRING,options={'type':'array','items':STRING},multiple_choice=BOOL,anonymous=BOOL),('chat','question','options'),True)
    async def poll(a):
        q=nonempty(a['question'],255);opts=a['options']
        if not 2<=len(opts)<=10 or any(not isinstance(o,str) or not o.strip() or len(o)>100 for o in opts) or len(set(opts))!=len(opts):raise ValueError('invalid_poll_options')
        cid=await api['resolve'](a['chat'])
        poll=T.Poll(id=0,hash=0,question=T.TextWithEntities(q,[]),answers=[T.PollAnswer(T.TextWithEntities(o,[]),str(i).encode()) for i,o in enumerate(opts)],multiple_choice=bool(a.get('multiple_choice')),public_voters=not a.get('anonymous',True))
        msg=await mutate(cid,lambda:api['S'].client.send_file(cid,T.InputMediaPoll(poll)))
        return {'sent':True,'chat_id':cid,'message_id':msg.id}

    async def fetch_poll(a):
        cid=await api['resolve'](a['chat']);api['require_monitoring'](cid)
        await api['sync_guard']();msg=await api['S'].client.get_messages(cid,ids=a['message_id'])
        api['require_monitoring'](cid)
        if not msg or not isinstance(msg.media,T.MessageMediaPoll):raise ValueError('poll_not_found')
        return cid,msg.media

    poll_args=dict(CHAT,message_id={'type':'integer'})
    @expose('send_poll_vote','Vota por el índice de las opciones; lista vacía retira el voto cuando Telegram lo permite.',dict(poll_args,options={'type':'array','items':{'type':'integer'}}),('chat','message_id','options'),True)
    async def vote(a):
        cid,media=await fetch_poll(a);opts=a['options']
        if any(type(i)is not int or not 0<=i<len(media.poll.answers) for i in opts) or len(set(opts))!=len(opts):raise ValueError('invalid_poll_options')
        if not media.poll.multiple_choice and len(opts)>1:raise ValueError('single_choice_poll')
        await rpc(cid,F.messages.SendVoteRequest(cid,a['message_id'],[media.poll.answers[i].option for i in opts]));return {'ok':True}

    @expose('get_poll_results','Consulta resultados de una encuesta, sujetos a visibilidad de Telegram.',poll_args,('chat','message_id'))
    async def results(a):
        _,media=await fetch_poll(a)
        counts={r.option:r.voters for r in media.results.results or []}
        return {'question':media.poll.question.text,'total_voters':media.results.total_voters,'options':[{'index':i,'text':answer.text.text,'votes':counts.get(answer.option)} for i,answer in enumerate(media.poll.answers)]}

    @expose('send_contact_card','Envía una tarjeta de contacto.',dict(CHAT,name=STRING,phone=STRING,vcard=STRING),('chat','name','phone'),True)
    async def contact(a):
        name=nonempty(a['name'],128);phone=nonempty(a['phone'],32);card=a.get('vcard','')
        if len(card)>16384:raise ValueError('vcard_too_large')
        cid=await api['resolve'](a['chat'])
        msg=await mutate(cid,lambda:api['S'].client.send_file(cid,T.InputMediaContact(phone,name,'',card)))
        return {'sent':True,'chat_id':cid,'message_id':msg.id}

    @expose('get_blocklist','Consulta bloqueados con paginación, hasta 100.',{'limit':{'type':'integer'},'offset':{'type':'integer'}})
    async def blocks(a):
        result=await api['S'].client(F.contacts.GetBlockedRequest(max(0,a.get('offset',0)),max(1,min(a.get('limit',50),100))))
        return {'blocked':[x.to_dict() for x in result.blocked],'count':getattr(result,'count',len(result.blocked))}

    for tool_name,request_type in [('block_contact',F.contacts.BlockRequest),('unblock_contact',F.contacts.UnblockRequest)]:
        def make_block(request_type):
            async def action(a):
                cid=await api['resolve'](a['chat']);await rpc(cid,request_type(cid));return {'ok':True}
            return action
        expose(tool_name,'Bloquea o desbloquea el contacto indicado.',CHAT,('chat',),True)(make_block(request_type))

    @expose('send_presence','Actualiza la presencia de esta sesión.',{'state':{'type':'string','enum':['available','unavailable']}},('state',),True)
    async def presence(a):
        await rpc(0,F.account.UpdateStatusRequest(offline=a['state']=='unavailable'));return {'ok':True}

    privacy_name={'type':'string','enum':list(PRIVACY)}
    @expose('get_privacy_settings','Consulta una regla de privacidad nativa de Telegram.',{'name':privacy_name},('name',))
    async def get_privacy(a):
        result=await api['S'].client(F.account.GetPrivacyRequest(PRIVACY[a['name']]()))
        return {'name':a['name'],'rules':[r.to_dict() for r in result.rules]}

    @expose('set_privacy_setting','Sustituye una regla de privacidad y sus excepciones por all, contacts o none.',{'name':privacy_name,'value':{'type':'string','enum':['all','contacts','none']}},('name','value'),True)
    async def set_privacy(a):
        rule={'all':T.InputPrivacyValueAllowAll,'contacts':T.InputPrivacyValueAllowContacts,'none':T.InputPrivacyValueDisallowAll}[a['value']]()
        await rpc(0,F.account.SetPrivacyRequest(PRIVACY[a['name']](),[rule]));return {'ok':True}

    @expose('set_status_message','Cambia la biografía del perfil, hasta 70 caracteres.',{'text':STRING},('text',),True)
    async def status(a):
        if len(a['text'])>70:raise ValueError('bio_too_long')
        await rpc(0,F.account.UpdateProfileRequest(about=a['text']));return {'ok':True}
